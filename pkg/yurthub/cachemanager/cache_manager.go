/*
Copyright 2020 The OpenYurt Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cachemanager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	meta2 "github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/meta"
	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/serializer"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/handlers"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog"
)

// CacheManager is an adaptor to cache runtime object data into backend storage
type CacheManager interface {
	CacheResponse(req *http.Request, prc io.ReadCloser, stopCh <-chan struct{}) error
	QueryCache(req *http.Request) (runtime.Object, error)
	UpdateCacheAgents(agents []string) error
	ListCacheAgents() []string
	CanCacheFor(req *http.Request) bool
}

type cacheManager struct {
	sync.RWMutex
	storage               StorageWrapper
	serializerManager     *serializer.SerializerManager
	restMapperManager     *meta2.RESTMapperManager
	cacheAgents           map[string]bool
	listSelectorCollector map[string]string
}

// NewCacheManager creates a new CacheManager
func NewCacheManager(
	storage StorageWrapper,
	serializerMgr *serializer.SerializerManager,
	restMapperMgr *meta2.RESTMapperManager,
) (CacheManager, error) {
	cm := &cacheManager{
		storage:               storage,
		serializerManager:     serializerMgr,
		restMapperManager:     restMapperMgr,
		cacheAgents:           make(map[string]bool),
		listSelectorCollector: make(map[string]string),
	}

	err := cm.initCacheAgents()
	if err != nil {
		return nil, err
	}

	return cm, nil
}

// CacheResponse cache response of request into backend storage
func (cm *cacheManager) CacheResponse(req *http.Request, prc io.ReadCloser, stopCh <-chan struct{}) error {
	ctx := req.Context()
	info, _ := apirequest.RequestInfoFrom(ctx)
	if isWatch(ctx) {
		return cm.saveWatchObject(ctx, info, prc, stopCh)
	}

	var buf bytes.Buffer
	n, err := buf.ReadFrom(prc)
	if err != nil {
		klog.Errorf("failed to cache response, %v", err)
		return err
	} else if n == 0 {
		err := fmt.Errorf("read 0-length data from response, %s", util.ReqInfoString(info))
		klog.Errorf("failed to cache response, %v", err)
		return err
	} else {
		klog.V(5).Infof("cache %d bytes from response for %s", n, util.ReqInfoString(info))
	}

	if isList(ctx) {
		return cm.saveListObject(ctx, info, buf.Bytes())
	}

	return cm.saveOneObject(ctx, info, buf.Bytes())
}

// QueryCache get runtime object from backend storage for request
func (cm *cacheManager) QueryCache(req *http.Request) (runtime.Object, error) {
	ctx := req.Context()
	info, ok := apirequest.RequestInfoFrom(ctx)
	if !ok || info == nil || info.Resource == "" {
		return nil, fmt.Errorf("failed to get request info")
	}

	comp, ok := util.ClientComponentFrom(ctx)
	if !ok || comp == "" {
		return nil, fmt.Errorf("failed to get component info")
	}

	if info.IsResourceRequest && info.Verb == "list" {
		return cm.queryListObject(req)
	} else if info.IsResourceRequest && (info.Verb == "get" || info.Verb == "patch" || info.Verb == "update") {
		key, err := util.KeyFunc(comp, info.Resource, info.Namespace, info.Name)
		if err != nil {
			return nil, err
		}
		return cm.storage.Get(key)
	}

	return nil, fmt.Errorf("request(%#+v) is not supported", info)
}

func (cm *cacheManager) queryListObject(req *http.Request) (runtime.Object, error) {
	ctx := req.Context()
	comp, _ := util.ClientComponentFrom(ctx)
	info, _ := apirequest.RequestInfoFrom(ctx)

	key, err := util.KeyFunc(comp, info.Resource, info.Namespace, info.Name)
	if err != nil {
		return nil, err
	}

	var gvk schema.GroupVersionKind
	var kind string
	objs, err := cm.storage.List(key)
	if err != nil {
		return nil, err
	} else if len(objs) == 0 {
		gvr := schema.GroupVersionResource{
			Group:    info.APIGroup,
			Version:  info.APIVersion,
			Resource: info.Resource,
		}
		_, gvk = cm.restMapperManager.KindFor(gvr)
		if gvk.Empty() {
			return nil, fmt.Errorf("no matches for %v", gvr)
		}
		kind = gvk.Kind
	} else {
		kind = objs[0].GetObjectKind().GroupVersionKind().Kind
	}

	var listObj runtime.Object
	listGvk := schema.GroupVersionKind{
		Group:   info.APIGroup,
		Version: info.APIVersion,
		Kind:    kind + "List",
	}
	if scheme.Scheme.Recognizes(listGvk) {
		listObj, err = scheme.Scheme.New(listGvk)
		if err != nil {
			klog.Errorf("failed to create list object(%v), %v", listGvk, err)
			return nil, err
		}
	} else {
		listObj = new(unstructured.UnstructuredList)
		listObj.GetObjectKind().SetGroupVersionKind(listGvk)
	}

	listRv := 0
	rvStr := ""
	rvInt := 0
	accessor := meta.NewAccessor()
	for i := range objs {
		rvStr, _ = accessor.ResourceVersion(objs[i])
		rvInt, _ = strconv.Atoi(rvStr)
		if rvInt > listRv {
			listRv = rvInt
		}
	}

	if err := meta.SetList(listObj, objs); err != nil {
		klog.Errorf("failed to meta set list with %d objects, %v", len(objs), err)
		return nil, err
	}

	accessor.SetResourceVersion(listObj, strconv.Itoa(listRv))
	err = setListObjSelfLink(listObj, req)
	return listObj, err
}

func setListObjSelfLink(listObj runtime.Object, req *http.Request) error {
	ctx := req.Context()
	info, _ := apirequest.RequestInfoFrom(ctx)
	clusterScoped := true
	if info.Namespace != "" {
		clusterScoped = false
	}

	prefix := "/" + path.Join(info.APIGroup, info.APIGroup)
	namer := handlers.ContextBasedNaming{
		SelfLinker:         runtime.SelfLinker(meta.NewAccessor()),
		SelfLinkPathPrefix: path.Join(prefix, info.Resource) + "/",
		SelfLinkPathSuffix: "",
		ClusterScoped:      clusterScoped,
	}

	uri, err := namer.GenerateListLink(req)
	if err != nil {
		return err
	}
	if err := namer.SetSelfLink(listObj, uri); err != nil {
		klog.Infof("Unable to set self link on object: %v", err)
	}

	return nil
}

func (cm *cacheManager) saveWatchObject(ctx context.Context, info *apirequest.RequestInfo, r io.ReadCloser, stopCh <-chan struct{}) error {
	delObjCnt := 0
	updateObjCnt := 0
	addObjCnt := 0

	comp, _ := util.ClientComponentFrom(ctx)
	respContentType, _ := util.RespContentTypeFrom(ctx)
	s := cm.serializerManager.CreateSerializer(respContentType, info.APIGroup, info.APIVersion, info.Resource)
	if s == nil {
		klog.Errorf("failed to create serializer in saveWatchObject, %s", util.ReqInfoString(info))
		return fmt.Errorf("failed to create serializer in saveWatchObject, %s", util.ReqInfoString(info))
	}

	accessor := meta.NewAccessor()

	d, err := s.WatchDecoder(r)
	if err != nil {
		klog.Errorf("saveWatchObject ended with error, %v", err)
		return err
	}

	defer func() {
		klog.Infof("%s watch %s: %s get %d objects(add:%d/update:%d/del:%d)", comp, info.Resource, info.Path, addObjCnt+updateObjCnt+delObjCnt, addObjCnt, updateObjCnt, delObjCnt)
	}()

	for {
		watchType, obj, err := d.Decode()
		if err != nil {
			klog.V(3).Infof("%s %s watch decode ended with: %v", comp, info.Path, err)
			return err
		}

		switch watchType {
		case watch.Added, watch.Modified, watch.Deleted:
			name, err := accessor.Name(obj)
			if err != nil || name == "" {
				klog.Errorf("failed to get name of watch object, %v", err)
				continue
			}

			ns, err := accessor.Namespace(obj)
			if err != nil {
				klog.Errorf("failed to get namespace of watch object, %v", err)
				continue
			}

			key, err := util.KeyFunc(comp, info.Resource, ns, name)
			if err != nil || key == "" {
				klog.Errorf("failed to get cache path, %v", err)
				continue
			}

			switch watchType {
			case watch.Added, watch.Modified:
				err = cm.saveOneObjectWithValidation(key, obj)
				if watchType == watch.Added {
					addObjCnt++
				} else {
					updateObjCnt++
				}
			case watch.Deleted:
				err = cm.storage.Delete(key)
				delObjCnt++
			default:
				// impossible go to here
			}

			if info.Resource == "pods" {
				klog.V(2).Infof("pod(%s) is %s", key, string(watchType))
			}

			if err == storage.ErrStorageAccessConflict {
				klog.V(2).Infof("skip to cache watch event because key(%s) is under processing", key)
			} else if err != nil {
				klog.Errorf("failed to process watch object %s, %v", key, err)
			}
		case watch.Bookmark:
			rv, _ := accessor.ResourceVersion(obj)
			klog.Infof("get bookmark with rv %s for %s watch %s", rv, comp, info.Resource)
		case watch.Error:
			klog.Infof("unable to understand watch event %#v", obj)
		}
	}
}

func (cm *cacheManager) saveListObject(ctx context.Context, info *apirequest.RequestInfo, b []byte) error {
	respContentType, _ := util.RespContentTypeFrom(ctx)
	s := cm.serializerManager.CreateSerializer(respContentType, info.APIGroup, info.APIVersion, info.Resource)
	if s == nil {
		klog.Errorf("failed to create serializer in saveListObject, %s", util.ReqInfoString(info))
		return fmt.Errorf("failed to create serializer in saveListObject, %s", util.ReqInfoString(info))
	}

	list, err := s.Decode(b)
	if err != nil || list == nil {
		klog.Errorf("failed to decode response in saveOneObject %v", err)
		return err
	}

	if _, ok := list.(*metav1.Status); ok {
		klog.Infof("it's not need to cache metav1.Status")
		return nil
	}

	items, err := meta.ExtractList(list)
	if err != nil {
		klog.Errorf("unable to understand list result %#v (%v)", list, err)
		return fmt.Errorf("unable to understand list result %#v (%v)", list, err)
	}
	klog.V(5).Infof("list items for %s is: %d", util.ReqInfoString(info), len(items))

	kind := strings.TrimSuffix(list.GetObjectKind().GroupVersionKind().Kind, "List")
	apiVersion := schema.GroupVersion{
		Group:   info.APIGroup,
		Version: info.APIVersion,
	}.String()
	accessor := meta.NewAccessor()

	// Verify if DynamicRESTMapper(which store the CRD info) needs to be updated
	if err := cm.restMapperManager.UpdateKind(schema.GroupVersionKind{Group: info.APIGroup, Version: info.APIVersion, Kind: kind}); err != nil {
		klog.Errorf("failed to update the DynamicRESTMapper %v", err)
	}

	comp, _ := util.ClientComponentFrom(ctx)
	// even if no objects in cloud cluster, we need to
	// make up a storage that represents the no resources
	// in local disk, so when cloud-edge network disconnected,
	// yurthub can return empty objects instead of 404 code(not found)
	if len(items) == 0 {
		// list returns no objects
		key, _ := util.KeyFunc(comp, info.Resource, info.Namespace, "")
		return cm.storage.Create(key, nil)
	} else if info.Name != "" && len(items) == 1 {
		// list with fieldSelector=metadata.name=xxx
		accessor.SetKind(items[0], kind)
		accessor.SetAPIVersion(items[0], apiVersion)
		name, _ := accessor.Name(items[0])
		ns, _ := accessor.Namespace(items[0])
		if ns == "" {
			ns = info.Namespace
		}
		key, _ := util.KeyFunc(comp, info.Resource, ns, name)
		err = cm.saveOneObjectWithValidation(key, items[0])
		if err == storage.ErrStorageAccessConflict {
			klog.V(2).Infof("skip to cache list object because key(%s) is under processing", key)
			return nil
		}

		return err
	} else {
		// list all objects or with fieldselector/labelselector
		rootKey, _ := util.KeyFunc(comp, info.Resource, info.Namespace, info.Name)
		objs := make(map[string]runtime.Object)
		for i := range items {
			accessor.SetKind(items[i], kind)
			accessor.SetAPIVersion(items[i], apiVersion)
			name, _ := accessor.Name(items[i])
			ns, _ := accessor.Namespace(items[i])
			if ns == "" {
				ns = info.Namespace
			}

			key, _ := util.KeyFunc(comp, info.Resource, ns, name)
			objs[key] = items[i]
		}

		return cm.storage.Replace(rootKey, objs)
	}
}

func (cm *cacheManager) saveOneObject(ctx context.Context, info *apirequest.RequestInfo, b []byte) error {
	comp, _ := util.ClientComponentFrom(ctx)
	respContentType, _ := util.RespContentTypeFrom(ctx)

	s := cm.serializerManager.CreateSerializer(respContentType, info.APIGroup, info.APIVersion, info.Resource)
	if s == nil {
		klog.Errorf("failed to create serializer in saveOneObject, %s", util.ReqInfoString(info))
		return fmt.Errorf("failed to create serializer in saveOneObject, %s", util.ReqInfoString(info))
	}

	obj, err := s.Decode(b)
	if err != nil {
		klog.Errorf("failed to decode response in saveOneObject(respContentType:%s): %s, %v", respContentType, util.ReqInfoString(info), err)
		return err
	} else if obj == nil {
		klog.Info("failed to decode nil object. skip cache")
		return nil
	} else if _, ok := obj.(*metav1.Status); ok {
		klog.Infof("it's not need to cache metav1.Status.")
		return nil
	}

	var name string
	accessor := meta.NewAccessor()
	if isCreate(ctx) {
		name, _ = accessor.Name(obj)
	} else {
		name = info.Name
	}

	if name == "" {
		klog.Errorf("cache object have no name, %s", info.Path)
		return nil
	}

	key, err := util.KeyFunc(comp, info.Resource, info.Namespace, name)
	if err != nil || key == "" {
		klog.Errorf("failed to get cache key(%s:%s:%s:%s), %v", comp, info.Resource, info.Namespace, info.Name, err)
		return err
	}

	// Verify if DynamicRESTMapper(which store the CRD info) needs to be updated
	gvk := obj.GetObjectKind().GroupVersionKind()
	if err := cm.restMapperManager.UpdateKind(gvk); err != nil {
		klog.Errorf("failed to update the DynamicRESTMapper %v", err)
	}

	if err := cm.saveOneObjectWithValidation(key, obj); err != nil {
		if err != storage.ErrStorageAccessConflict {
			return err
		}
		klog.V(2).Infof("skip to cache object because key(%s) is under processing", key)
	}

	return nil
}

func (cm *cacheManager) saveOneObjectWithValidation(key string, obj runtime.Object) error {
	accessor := meta.NewAccessor()
	if isNotAssignedPod(obj) {
		ns, _ := accessor.Namespace(obj)
		name, _ := accessor.Name(obj)
		return fmt.Errorf("pod(%s/%s) is not assigned to a node, skip cache it.", ns, name)
	}

	oldObj, err := cm.storage.Get(key)
	if err == nil && oldObj != nil {
		oldRv, err := accessor.ResourceVersion(oldObj)
		if err != nil {
			klog.Errorf("failed to get old object resource version for %s, %v", key, err)
			return err
		}

		newRv, err := accessor.ResourceVersion(obj)
		if err != nil {
			klog.Errorf("failed to get new object resource version for %s, %v", key, err)
			return err
		}

		oldRvInt, _ := strconv.Atoi(oldRv)
		newRvInt, _ := strconv.Atoi(newRv)
		if newRvInt <= oldRvInt { // resource version is incremented or not
			return nil
		}

		return cm.storage.Update(key, obj)
	} else if os.IsNotExist(err) || oldObj == nil {
		return cm.storage.Create(key, obj)
	} else {
		if err != storage.ErrStorageAccessConflict {
			return cm.storage.Create(key, obj)
		}
		return err
	}
}

// isNotAssignedPod check pod is assigned to node or not
// when delete pod of statefulSet, kubelet may get pod unassigned.
func isNotAssignedPod(obj runtime.Object) bool {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return false
	}

	if pod.Spec.NodeName == "" {
		return true
	}

	return false
}

func isList(ctx context.Context) bool {
	if info, ok := apirequest.RequestInfoFrom(ctx); ok {
		return info.Verb == "list"
	}

	return false
}

func isWatch(ctx context.Context) bool {
	if info, ok := apirequest.RequestInfoFrom(ctx); ok {
		return info.Verb == "watch"
	}

	return false
}

func isCreate(ctx context.Context) bool {
	if info, ok := apirequest.RequestInfoFrom(ctx); ok {
		return info.Verb == "create"
	}

	return false
}

// CanCacheFor checks response of request can be cached or not
// the following request is not supported to cache response
// 1. component is not set
// 2. delete/deletecollection/proxy request
// 3. sub-resource request but is not status
// 4. csr resource request
func (cm *cacheManager) CanCacheFor(req *http.Request) bool {
	ctx := req.Context()
	comp, ok := util.ClientComponentFrom(ctx)
	if !ok || len(comp) == 0 {
		return false
	}

	canCache, ok := util.ReqCanCacheFrom(ctx)
	if ok && canCache {
		// request with Edge-Cache header, continue verification
	} else {
		cm.RLock()
		if _, found := cm.cacheAgents[comp]; !found {
			cm.RUnlock()
			return false
		}
		cm.RUnlock()
	}

	info, ok := apirequest.RequestInfoFrom(ctx)
	if !ok || info == nil {
		return false
	}

	if !info.IsResourceRequest {
		return false
	}

	if info.Verb == "delete" || info.Verb == "deletecollection" || info.Verb == "proxy" {
		return false
	}

	if info.Subresource != "" && info.Subresource != "status" {
		return false
	}

	cm.Lock()
	defer cm.Unlock()
	if info.Verb == "list" && info.Name == "" {
		key, _ := util.KeyFunc(comp, info.Resource, info.Namespace, info.Name)
		selector, _ := util.ListSelectorFrom(ctx)
		if oldSelector, ok := cm.listSelectorCollector[key]; ok {
			if oldSelector != selector {
				// list requests that have the same path but with different selector, for example:
				// request1: http://{ip:port}/api/v1/default/pods?labelSelector=foo=bar
				// request2: http://{ip:port}/api/v1/default/pods?labelSelector=foo2=bar2
				// because func queryListObject() will get all pods for both requests instead of
				// getting pods by request selector. so cache manager can not support same path list
				// requests that has different selector.
				klog.Warningf("list requests that have the same path but with different selector, skip cache for %s", util.ReqString(req))
				return false
			}
		} else {
			// list requests that get the same resources but with different path, for example:
			// request1: http://{ip/port}/api/v1/pods?fieldSelector=spec.nodeName=foo
			// request2: http://{ip/port}/api/v1/default/pods?fieldSelector=spec.nodeName=foo
			// because func queryListObject() will get all pods for both requests instead of
			// getting pods by request selector. so cache manager can not support getting same resource
			// list requests that has different path.
			for k := range cm.listSelectorCollector {
				if (len(k) > len(key) && strings.Contains(k, key)) || (len(k) < len(key) && strings.Contains(key, k)) {
					klog.Warningf("list requests that get the same resources but with different path, skip cache for %s", util.ReqString(req))
					return false
				}
			}
			cm.listSelectorCollector[key] = selector
		}
	}
	return true
}
