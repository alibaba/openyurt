/*
Copyright 2021 The OpenYurt Authors.

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

package meta

import (
	normaljson "encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage/disk"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog"
)

const (
	CacheDynamicRESTMapperKey = "_internal/restmapper/cache-crd-restmapper.conf"
	SepForGVR                 = "/"
)

var (
	// unsafeDefaultRESTMapper is used to store the mapping relationship between GVK and GVR in scheme
	unsafeDefaultRESTMapper = NewDefaultRESTMapperFromScheme()
)

// RESTMapperManager is responsible for managing different kind of RESTMapper
type RESTMapperManager struct {
	sync.RWMutex
	storage storage.Store
	// UnsafeDefaultRESTMapper is used to save the GVK and GVR mapping relationships of built-in resources
	unsafeDefaultRESTMapper *meta.DefaultRESTMapper
	// dynamicRESTMapper is used to save the GVK and GVR mapping relationships of Custom Resources
	dynamicRESTMapper map[schema.GroupVersionResource]schema.GroupVersionKind
}

func NewDefaultRESTMapperFromScheme() *meta.DefaultRESTMapper {
	s := scheme.Scheme
	defaultGroupVersions := s.PrioritizedVersionsAllGroups()
	mapper := meta.NewDefaultRESTMapper(defaultGroupVersions)
	// enumerate all supported versions, get the kinds, and register with the mapper how to address
	// our resources.
	for _, gv := range defaultGroupVersions {
		for kind := range s.KnownTypes(gv) {
			// Only need to process non-list resources
			if !strings.HasSuffix(kind, "List") {
				// Since RESTMapper is only used for mapping GVR to GVK information,
				// the scope field is not involved in actual use,
				// so all scope are currently set to meta.RESTScopeNamespace
				scope := meta.RESTScopeNamespace
				mapper.Add(gv.WithKind(kind), scope)
			}
		}
	}
	return mapper
}

func NewRESTMapperManager(rootDir string) *RESTMapperManager {
	dm := make(map[schema.GroupVersionResource]schema.GroupVersionKind)
	dStorage, err := disk.NewDiskStorage(rootDir)
	if err == nil {
		// Recover the mapping relationship between GVR and GVK from the hard disk
		b, err := dStorage.Get(CacheDynamicRESTMapperKey)
		if err == nil && len(b) != 0 {
			cacheMapper := make(map[string]string)
			err = normaljson.Unmarshal(b, &cacheMapper)
			if err != nil {
				klog.Errorf("failed to get cached CRDRESTMapper, %v", err)
			}

			for gvrString, gvkString := range cacheMapper {
				localInfo := strings.Split(gvrString, SepForGVR)
				if len(localInfo) != 3 {
					klog.Errorf("This %s is not standardized", gvrString)
					continue
				}
				gvr := schema.GroupVersionResource{
					Group:    localInfo[0],
					Version:  localInfo[1],
					Resource: localInfo[2],
				}
				gvk := schema.GroupVersionKind{
					Group:   localInfo[0],
					Version: localInfo[1],
					Kind:    gvkString,
				}
				dm[gvr] = gvk
			}
			klog.Infof("reset DynamicRESTMapper to %v", cacheMapper)
		} else {
			klog.Infof("initialize an empty DynamicRESTMapper")
		}
	} else {
		klog.Errorf("failed to create disk storage, %v, only initialize an empty DynamicRESTMapper in memory ", err)
	}

	return &RESTMapperManager{
		unsafeDefaultRESTMapper: unsafeDefaultRESTMapper,
		dynamicRESTMapper:       dm,
		storage:                 dStorage,
	}
}

// Obtain gvk according to gvr in dynamicRESTMapper
func (rm *RESTMapperManager) dynamicKindFor(gvr schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	rm.RLock()
	defer rm.RUnlock()
	resource := gvr

	hasResource := len(resource.Resource) > 0
	hasGroup := len(resource.Group) > 0
	hasVersion := len(resource.Version) > 0

	if !hasResource || !hasGroup {
		return schema.GroupVersionKind{}, fmt.Errorf("a resource and group must be present, got: %v", resource)
	}

	if hasVersion {
		// fully qualified. Find the exact match
		kind, exists := rm.dynamicRESTMapper[resource]
		if exists {
			return kind, nil
		}
	} else {
		requestedGroupResource := resource.GroupResource()
		for currResource, currKind := range rm.dynamicRESTMapper {
			if currResource.GroupResource() == requestedGroupResource {
				return currKind, nil
			}
		}
	}
	return schema.GroupVersionKind{}, fmt.Errorf("no matches for %v", resource)
}

// Used to delete the mapping relationship between GVR and GVK in dynamicRESTMapper
func (rm *RESTMapperManager) deleteKind(gvk schema.GroupVersionKind) error {
	rm.Lock()
	kindName := strings.TrimSuffix(gvk.Kind, "List")
	plural, singular := meta.UnsafeGuessKindToResource(gvk.GroupVersion().WithKind(kindName))
	delete(rm.dynamicRESTMapper, plural)
	delete(rm.dynamicRESTMapper, singular)
	rm.Unlock()
	return rm.updateCachedDynamicRESTMapper()
}

// Used to update local files saved on disk
func (rm *RESTMapperManager) updateCachedDynamicRESTMapper() error {
	if rm.storage == nil {
		return nil
	}
	cacheMapper := make(map[string]string, len(rm.dynamicRESTMapper))
	rm.RLock()
	for currResource, currKind := range rm.dynamicRESTMapper {
		//key: Group/Version/Resource, value: Kind
		k := strings.Join([]string{currResource.Group, currResource.Version, currResource.Resource}, SepForGVR)
		cacheMapper[k] = currKind.Kind
	}
	rm.RUnlock()
	d, err := normaljson.Marshal(cacheMapper)
	if err != nil {
		return err
	}
	return rm.storage.Update(CacheDynamicRESTMapperKey, d)
}

// KindFor is used to find GVK based on GVR information.
// 1. return true means the GVR is a built-in resource in shceme.
// 2.1 return false and non-empty GVK means the GVR is custom resource
// 2.2 return false and empty GVK means the GVR is unknown resource.
func (rm *RESTMapperManager) KindFor(gvr schema.GroupVersionResource) (bool, schema.GroupVersionKind) {
	gvk, kindErr := rm.unsafeDefaultRESTMapper.KindFor(gvr)
	if kindErr != nil {
		gvk, kindErr = rm.dynamicKindFor(gvr)
		if kindErr != nil {
			return false, schema.GroupVersionKind{}
		}
		return false, gvk
	}
	return true, gvk
}

// DeleteKindFor is used to delete the GVK information related to the incoming gvr
func (rm *RESTMapperManager) DeleteKindFor(gvr schema.GroupVersionResource) error {
	isScheme, gvk := rm.KindFor(gvr)
	if !isScheme && !gvk.Empty() {
		return rm.deleteKind(gvk)
	}
	return nil
}

// UpdateKind is used to verify and add the GVK and GVR mapping relationships of new Custom Resource
func (rm *RESTMapperManager) UpdateKind(gvk schema.GroupVersionKind) error {
	kindName := strings.TrimSuffix(gvk.Kind, "List")
	gvk = gvk.GroupVersion().WithKind(kindName)
	plural, singular := meta.UnsafeGuessKindToResource(gvk.GroupVersion().WithKind(kindName))
	// If it is not a built-in resource and it is not stored in DynamicRESTMapper, add it to DynamicRESTMapper
	isScheme, t := rm.KindFor(singular)
	if !isScheme && t.Empty() {
		rm.Lock()
		rm.dynamicRESTMapper[singular] = gvk
		rm.dynamicRESTMapper[plural] = gvk
		rm.Unlock()
		return rm.updateCachedDynamicRESTMapper()
	}
	return nil
}

// IsSchemeResource is used to determine whether gvr is a built-in resource
func IsSchemeResource(gvr schema.GroupVersionResource) bool {
	_, kindErr := unsafeDefaultRESTMapper.KindFor(gvr)
	if kindErr != nil {
		return false
	}
	return true
}
