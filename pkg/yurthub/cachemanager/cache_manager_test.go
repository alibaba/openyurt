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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openyurtio/openyurt/pkg/projectinfo"
	meta2 "github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/meta"
	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/serializer"
	proxyutil "github.com/openyurtio/openyurt/pkg/yurthub/proxy/util"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage/disk"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"

	v1 "k8s.io/api/core/v1"
	nodev1beta1 "k8s.io/api/node/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/filters"
)

var (
	rootDir = "/tmp/cache-manager"
)

func TestCacheGetResponse(t *testing.T) {
	dStorage, err := disk.NewDiskStorage(rootDir)
	restRESTMapperMgr := meta2.NewRESTMapperManager(rootDir)
	if err != nil {
		t.Errorf("failed to create disk storage, %v", err)
	}
	sWrapper := NewStorageWrapper(dStorage)
	serializerM := serializer.NewSerializerManager()
	yurtCM := &cacheManager{
		storage:           sWrapper,
		serializerManager: serializerM,
		cacheAgents:       make(map[string]bool),
		restMapperManager: restRESTMapperMgr,
	}

	testcases := map[string]struct {
		group        string
		version      string
		key          string
		inputObj     runtime.Object
		userAgent    string
		accept       string
		verb         string
		path         string
		resource     string
		namespaced   bool
		expectResult struct {
			err  error
			rv   string
			name string
			ns   string
			kind string
		}
		cacheResponseErr bool
	}{
		"cache response for pod with not assigned node": {
			group:   "",
			version: "v1",
			key:     "kubelet/pods/default/mypod1",
			inputObj: runtime.Object(&v1.Pod{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Pod",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mypod1",
					Namespace:       "default",
					ResourceVersion: "1",
				},
			}),
			userAgent:        "kubelet",
			accept:           "application/json",
			verb:             "GET",
			path:             "/api/v1/namespaces/default/pods/mypod1",
			resource:         "pods",
			namespaced:       true,
			cacheResponseErr: true,
		},
		"cache response for get pod": {
			group:   "",
			version: "v1",
			key:     "kubelet/pods/default/mypod1",
			inputObj: runtime.Object(&v1.Pod{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Pod",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mypod1",
					Namespace:       "default",
					ResourceVersion: "1",
				},
				Spec: v1.PodSpec{
					NodeName: "node1",
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods/mypod1",
			resource:   "pods",
			namespaced: true,
			expectResult: struct {
				err  error
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "1",
				name: "mypod1",
				ns:   "default",
				kind: "Pod",
			},
		},
		"cache response for get pod2": {
			group:   "",
			version: "v1",
			key:     "kubelet/pods/default/mypod2",
			inputObj: runtime.Object(&v1.Pod{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Pod",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mypod2",
					Namespace:       "default",
					ResourceVersion: "3",
				},
				Spec: v1.PodSpec{
					NodeName: "node1",
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods/mypod2",
			resource:   "pods",
			namespaced: true,
			expectResult: struct {
				err  error
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "3",
				name: "mypod2",
				ns:   "default",
				kind: "Pod",
			},
		},
		"cache response for get node": {
			group:   "",
			version: "v1",
			key:     "kubelet/nodes/mynode1",
			inputObj: runtime.Object(&v1.Node{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Node",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mynode1",
					ResourceVersion: "4",
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/nodes/mynode1",
			resource:   "nodes",
			namespaced: false,
			expectResult: struct {
				err  error
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "4",
				name: "mynode1",
				kind: "Node",
			},
		},
		"cache response for get node2": {
			group:   "",
			version: "v1",
			key:     "kubelet/nodes/mynode2",
			inputObj: runtime.Object(&v1.Node{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Node",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mynode2",
					ResourceVersion: "6",
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/nodes/mynode2",
			resource:   "nodes",
			namespaced: false,
			expectResult: struct {
				err  error
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "6",
				name: "mynode2",
				kind: "Node",
			},
		},
		//used to test whether custom resources can be cached correctly
		"cache response for get crontab": {
			group:   "stable.example.com",
			version: "v1",
			key:     "kubelet/crontabs/default/crontab1",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "stable.example.com/v1",
					"kind":       "CronTab",
					"metadata": map[string]interface{}{
						"name":            "crontab1",
						"namespace":       "default",
						"resourceVersion": "1",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs/crontab1",
			resource:   "crontabs",
			namespaced: true,
			expectResult: struct {
				err  error
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "1",
				name: "crontab1",
				ns:   "default",
				kind: "CronTab",
			},
		},
		"cache response for get crontab2": {
			group:   "stable.example.com",
			version: "v1",
			key:     "kubelet/crontabs/default/crontab2",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "stable.example.com/v1",
					"kind":       "CronTab",
					"metadata": map[string]interface{}{
						"name":            "crontab2",
						"namespace":       "default",
						"resourceVersion": "3",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs/crontab2",
			resource:   "crontabs",
			namespaced: true,
			expectResult: struct {
				err  error
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "3",
				name: "crontab2",
				ns:   "default",
				kind: "CronTab",
			},
		},
		"cache response for get foo without namespace": {
			group:   "samplecontroller.k8s.io",
			version: "v1",
			key:     "kubelet/foos/foo1",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "samplecontroller.k8s.io/v1",
					"kind":       "Foo",
					"metadata": map[string]interface{}{
						"name":            "foo1",
						"resourceVersion": "3",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/samplecontroller.k8s.io/v1/foos/foo1",
			resource:   "foos",
			namespaced: false,
			expectResult: struct {
				err  error
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "3",
				name: "foo1",
				kind: "Foo",
			},
		},
		"cache response for get foo2 without namespace": {
			group:   "samplecontroller.k8s.io",
			version: "v1",
			key:     "kubelet/foos/foo2",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "samplecontroller.k8s.io/v1",
					"kind":       "Foo",
					"metadata": map[string]interface{}{
						"name":            "foo2",
						"resourceVersion": "5",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/samplecontroller.k8s.io/v1/foos/foo2",
			resource:   "foos",
			namespaced: false,
			expectResult: struct {
				err  error
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "5",
				name: "foo2",
				kind: "Foo",
			},
		},
		"cache response for Status": {
			group:   "",
			version: "v1",
			key:     "kubelet/nodes/test",
			inputObj: runtime.Object(&metav1.Status{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Status",
				},
				Status:  "Failure",
				Message: "node test is not exist",
				Reason:  "NotFound",
				Code:    404,
			}),
			userAgent: "kubelet",
			accept:    "application/json",
			verb:      "GET",
			path:      "/api/v1/nodes/test",
			resource:  "nodes",
			expectResult: struct {
				err  error
				rv   string
				name string
				ns   string
				kind string
			}{
				err: storage.ErrStorageNotFound,
			},
		},
		"cache response for nil object": {
			group:            "",
			version:          "v1",
			key:              "kubelet/nodes/test",
			inputObj:         nil,
			userAgent:        "kubelet",
			accept:           "application/json",
			verb:             "GET",
			path:             "/api/v1/nodes/test",
			resource:         "nodes",
			cacheResponseErr: true,
		},
	}

	accessor := meta.NewAccessor()
	resolver := newTestRequestInfoResolver()
	for k, tt := range testcases {
		t.Run(k, func(t *testing.T) {
			s := serializerM.CreateSerializer(tt.accept, tt.group, tt.version, tt.resource)
			encoder, err := s.Encoder(tt.accept, nil)
			if err != nil {
				t.Fatalf("could not create encoder, %v", err)
			}

			buf := bytes.NewBuffer([]byte{})
			if tt.inputObj != nil {
				err = encoder.Encode(tt.inputObj, buf)
				if err != nil {
					t.Fatalf("could not encode input object, %v", err)
				}
			}

			req, _ := http.NewRequest(tt.verb, tt.path, nil)
			if len(tt.userAgent) != 0 {
				req.Header.Set("User-Agent", tt.userAgent)
			}

			if len(tt.accept) != 0 {
				req.Header.Set("Accept", tt.accept)
			}
			req.RemoteAddr = "127.0.0.1"

			var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				ctx := req.Context()
				ctx = util.WithRespContentType(ctx, tt.accept)
				req = req.WithContext(ctx)
				prc := ioutil.NopCloser(buf)
				err = yurtCM.CacheResponse(req, prc, nil)
			})

			handler = proxyutil.WithRequestContentType(handler)
			handler = proxyutil.WithRequestClientComponent(handler)
			handler = filters.WithRequestInfo(handler, resolver)
			handler.ServeHTTP(httptest.NewRecorder(), req)

			if tt.cacheResponseErr && err == nil {
				t.Errorf("expect err, but do not get error")
			} else if !tt.cacheResponseErr && err != nil {
				t.Errorf("expect no err, but got error %v", err)
			}

			if len(tt.expectResult.name) == 0 {
				return
			}

			obj, err := sWrapper.Get(tt.key)
			if err != nil || obj == nil {
				if tt.expectResult.err != err {
					t.Errorf("expect get error %v, but got %v", tt.expectResult.err, err)
				}
				t.Logf("get expected err %v for key %s", tt.expectResult.err, tt.key)
			} else {
				name, _ := accessor.Name(obj)
				rv, _ := accessor.ResourceVersion(obj)
				kind, _ := accessor.Kind(obj)
				if tt.expectResult.name != name {
					t.Errorf("Got name %s, but expect name %s", name, tt.expectResult.name)
				}

				if tt.expectResult.rv != rv {
					t.Errorf("Got rv %s, but expect rv %s", rv, tt.expectResult.rv)
				}

				if tt.namespaced {
					ns, _ := accessor.Namespace(obj)
					if tt.expectResult.ns != ns {
						t.Errorf("Got ns %s, but expect ns %s", ns, tt.expectResult.ns)
					}
				}

				if tt.expectResult.kind != kind {
					t.Errorf("Got kind %s, but expect kind %s", kind, tt.expectResult.kind)
				}
				t.Logf("get key %s successfully", tt.key)
			}

			err = sWrapper.DeleteCollection("kubelet")
			if err != nil {
				t.Errorf("failed to delete collection: kubelet, %v", err)
			}
			ds, _ := disk.NewDiskStorage(rootDir)
			if err = resetRESTMapper(ds, yurtCM.restMapperManager); err != nil {
				t.Errorf("failed to delete cached DynamicRESTMapper, %v", err)
			}
		})
	}
}

func TestCacheWatchResponse(t *testing.T) {
	mkPod := func(id string, rv string) *v1.Pod {
		return &v1.Pod{
			TypeMeta:   metav1.TypeMeta{APIVersion: "", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: "default", ResourceVersion: rv},
			Spec:       v1.PodSpec{NodeName: "node1"},
		}
	}

	//used to generate the custom resources
	mkCronTab := func(id string, rv string) *unstructured.Unstructured {
		return &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "stable.example.com/v1",
				"kind":       "CronTab",
				"metadata": map[string]interface{}{
					"name":            id,
					"namespace":       "default",
					"resourceVersion": rv,
				},
			},
		}
	}

	dStorage, err := disk.NewDiskStorage(rootDir)
	restRESTMapperMgr := meta2.NewRESTMapperManager(rootDir)
	if err != nil {
		t.Errorf("failed to create disk storage, %v", err)
	}
	sWrapper := NewStorageWrapper(dStorage)
	serializerM := serializer.NewSerializerManager()
	yurtCM := &cacheManager{
		storage:           sWrapper,
		serializerManager: serializerM,
		cacheAgents:       make(map[string]bool),
		restMapperManager: restRESTMapperMgr,
	}

	testcases := map[string]struct {
		group        string
		version      string
		key          string
		inputObj     []watch.Event
		userAgent    string
		accept       string
		verb         string
		path         string
		resource     string
		namespaced   bool
		expectResult struct {
			err  bool
			data map[string]struct{}
		}
	}{
		"add pods": {
			group:   "",
			version: "v1",
			key:     "kubelet/pods/default",
			inputObj: []watch.Event{
				{Type: watch.Added, Object: mkPod("mypod1", "2")},
				{Type: watch.Added, Object: mkPod("mypod2", "4")},
				{Type: watch.Added, Object: mkPod("mypod3", "6")},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods?watch=true",
			resource:   "pods",
			namespaced: true,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"pod-default-mypod1-2": {},
					"pod-default-mypod2-4": {},
					"pod-default-mypod3-6": {},
				},
			},
		},
		"add and delete pods": {
			group:   "",
			version: "v1",
			key:     "kubelet/pods/default",
			inputObj: []watch.Event{
				{Type: watch.Added, Object: mkPod("mypod1", "2")},
				{Type: watch.Deleted, Object: mkPod("mypod1", "4")},
				{Type: watch.Added, Object: mkPod("mypod3", "6")},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods?watch=true",
			namespaced: true,
			resource:   "pods",
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"pod-default-mypod3-6": {},
				},
			},
		},
		"add and update pods": {
			group:   "",
			version: "v1",
			key:     "kubelet/pods/default",
			inputObj: []watch.Event{
				{Type: watch.Added, Object: mkPod("mypod1", "2")},
				{Type: watch.Modified, Object: mkPod("mypod1", "4")},
				{Type: watch.Added, Object: mkPod("mypod3", "6")},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods?watch=true",
			resource:   "pods",
			namespaced: true,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"pod-default-mypod1-4": {},
					"pod-default-mypod3-6": {},
				},
			},
		},
		"not update pods": {
			group:   "",
			version: "v1",
			key:     "kubelet/pods/default",
			inputObj: []watch.Event{
				{Type: watch.Added, Object: mkPod("mypod1", "6")},
				{Type: watch.Modified, Object: mkPod("mypod1", "4")},
				{Type: watch.Modified, Object: mkPod("mypod1", "2")},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods?watch=true",
			resource:   "pods",
			namespaced: true,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"pod-default-mypod1-6": {},
				},
			},
		},
		//used to test whether custom resource's watch-events can be cached correctly
		"cache response for watch add crontabs": {
			group:   "stable.example.com",
			version: "v1",
			key:     "kubelet/crontabs/default",
			inputObj: []watch.Event{
				{Type: watch.Added, Object: mkCronTab("crontab1", "2")},
				{Type: watch.Added, Object: mkCronTab("crontab2", "4")},
				{Type: watch.Added, Object: mkCronTab("crontab3", "6")},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs?watch=true",
			resource:   "crontabs",
			namespaced: true,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"crontab-default-crontab1-2": {},
					"crontab-default-crontab2-4": {},
					"crontab-default-crontab3-6": {},
				},
			},
		},
		"cache response for watch add and delete crontabs": {
			group:   "stable.example.com",
			version: "v1",
			key:     "kubelet/crontabs/default",
			inputObj: []watch.Event{
				{Type: watch.Added, Object: mkCronTab("crontab1", "2")},
				{Type: watch.Deleted, Object: mkCronTab("crontab1", "4")},
				{Type: watch.Added, Object: mkCronTab("crontab3", "6")},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs?watch=true",
			resource:   "crontabs",
			namespaced: true,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"crontab-default-crontab3-6": {},
				},
			},
		},
		"cache response for watch add and update crontabs": {
			group:   "stable.example.com",
			version: "v1",
			key:     "kubelet/crontabs/default",
			inputObj: []watch.Event{
				{Type: watch.Added, Object: mkCronTab("crontab1", "2")},
				{Type: watch.Modified, Object: mkCronTab("crontab1", "4")},
				{Type: watch.Added, Object: mkCronTab("crontab3", "6")},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs?watch=true",
			resource:   "crontabs",
			namespaced: true,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"crontab-default-crontab1-4": {},
					"crontab-default-crontab3-6": {},
				},
			},
		},
		"cache response for watch not update crontabs": {
			group:   "stable.example.com",
			version: "v1",
			key:     "kubelet/crontabs/default",
			inputObj: []watch.Event{
				{Type: watch.Added, Object: mkCronTab("crontab1", "6")},
				{Type: watch.Modified, Object: mkCronTab("crontab1", "4")},
				{Type: watch.Modified, Object: mkCronTab("crontab1", "2")},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs?watch=true",
			resource:   "crontabs",
			namespaced: true,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"crontab-default-crontab1-6": {},
				},
			},
		},
	}

	resolver := newTestRequestInfoResolver()
	for k, tt := range testcases {
		t.Run(k, func(t *testing.T) {
			s := serializerM.CreateSerializer(tt.accept, tt.group, tt.version, tt.resource)
			r, w := io.Pipe()
			go func(w *io.PipeWriter) {
				//For unregistered GVKs, the normal encoding is used by default and the original GVK information is set

				for i := range tt.inputObj {
					if _, err := s.WatchEncode(w, &tt.inputObj[i]); err != nil {
						t.Errorf("%d: encode watch unexpected error: %v", i, err)
						continue
					}
					time.Sleep(100 * time.Millisecond)
				}
				w.Close()
			}(w)

			req, _ := http.NewRequest(tt.verb, tt.path, nil)
			if len(tt.userAgent) != 0 {
				req.Header.Set("User-Agent", tt.userAgent)
			}

			if len(tt.accept) != 0 {
				req.Header.Set("Accept", tt.accept)
			}
			req.RemoteAddr = "127.0.0.1"

			var err error
			rc := ioutil.NopCloser(r)
			var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				ctx := req.Context()
				ctx = util.WithRespContentType(ctx, tt.accept)
				req = req.WithContext(ctx)
				err = yurtCM.CacheResponse(req, rc, nil)
			})

			handler = proxyutil.WithRequestContentType(handler)
			handler = proxyutil.WithRequestClientComponent(handler)
			handler = filters.WithRequestInfo(handler, resolver)
			handler.ServeHTTP(httptest.NewRecorder(), req)

			if tt.expectResult.err && err == nil {
				t.Errorf("expect err, but do not got err")
			} else if err != nil && err != io.EOF {
				t.Errorf("failed to cache resposne, %v", err)
			}

			if len(tt.expectResult.data) == 0 {
				return
			}

			objs, err := sWrapper.List(tt.key)
			if err != nil || len(objs) == 0 {
				t.Errorf("failed to get object from storage")
			}

			if !compareObjectsAndKeys(t, objs, tt.namespaced, tt.expectResult.data) {
				t.Errorf("got unexpected objects for keys for watch request")
			}

			err = sWrapper.DeleteCollection("kubelet")
			if err != nil {
				t.Errorf("failed to delete collection: kubelet, %v", err)
			}
			ds, _ := disk.NewDiskStorage(rootDir)
			if err = resetRESTMapper(ds, yurtCM.restMapperManager); err != nil {
				t.Errorf("failed to delete cached DynamicRESTMapper, %v", err)
			}
		})
	}
}

func TestCacheListResponse(t *testing.T) {
	dStorage, err := disk.NewDiskStorage(rootDir)
	if err != nil {
		t.Errorf("failed to create disk storage, %v", err)
	}
	sWrapper := NewStorageWrapper(dStorage)

	serializerM := serializer.NewSerializerManager()
	restRESTMapperMgr := meta2.NewRESTMapperManager(rootDir)
	yurtCM := &cacheManager{
		storage:           sWrapper,
		serializerManager: serializerM,
		cacheAgents:       make(map[string]bool),
		restMapperManager: restRESTMapperMgr,
	}

	testcases := map[string]struct {
		group        string
		version      string
		key          string
		inputObj     runtime.Object
		userAgent    string
		accept       string
		verb         string
		path         string
		resource     string
		namespaced   bool
		expectResult struct {
			err  bool
			data map[string]struct{}
		}
		cacheErr error
	}{
		"list pods": {
			group:   "",
			version: "v1",
			key:     "kubelet/pods/default",
			inputObj: runtime.Object(
				&v1.PodList{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "PodList",
					},
					ListMeta: metav1.ListMeta{
						ResourceVersion: "5",
					},
					Items: []v1.Pod{
						{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "v1",
								Kind:       "Pod",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:            "mypod1",
								Namespace:       "default",
								ResourceVersion: "1",
							},
						},
						{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "v1",
								Kind:       "Pod",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:            "mypod2",
								Namespace:       "default",
								ResourceVersion: "3",
							},
						},
						{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "v1",
								Kind:       "Pod",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:            "mypod3",
								Namespace:       "default",
								ResourceVersion: "5",
							},
						},
					},
				},
			),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods",
			resource:   "pods",
			namespaced: true,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"pod-default-mypod1-1": {},
					"pod-default-mypod2-3": {},
					"pod-default-mypod3-5": {},
				},
			},
		},
		"list nodes": {
			group:   "",
			version: "v1",
			key:     "kubelet/nodes",
			inputObj: runtime.Object(
				&v1.NodeList{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "NodeList",
					},
					ListMeta: metav1.ListMeta{
						ResourceVersion: "12",
					},
					Items: []v1.Node{
						{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "v1",
								Kind:       "Node",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:            "mynode1",
								ResourceVersion: "6",
							},
						},
						{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "v1",
								Kind:       "Node",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:            "mynode2",
								ResourceVersion: "8",
							},
						},
						{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "v1",
								Kind:       "Node",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:            "mynode3",
								ResourceVersion: "10",
							},
						},
						{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "v1",
								Kind:       "Node",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:            "mynode4",
								ResourceVersion: "12",
							},
						},
					},
				},
			),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/nodes",
			resource:   "nodes",
			namespaced: false,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"node-mynode1-6":  {},
					"node-mynode2-8":  {},
					"node-mynode3-10": {},
					"node-mynode4-12": {},
				},
			},
		},
		"list nodes with fieldselector": {
			group:   "",
			version: "v1",
			key:     "kubelet/nodes",
			inputObj: runtime.Object(
				&v1.NodeList{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "NodeList",
					},
					ListMeta: metav1.ListMeta{
						ResourceVersion: "12",
					},
					Items: []v1.Node{
						{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "v1",
								Kind:       "Node",
							},
							ObjectMeta: metav1.ObjectMeta{
								Name:            "mynode",
								ResourceVersion: "12",
							},
						},
					},
				},
			),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/nodes?fieldselector=meatadata.name=mynode",
			resource:   "nodes",
			namespaced: false,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"node-mynode-12": {},
				},
			},
		},
		"list runtimeclasses with no objects": {
			group:   "node.k8s.io",
			version: "v1beta1",
			key:     "kubelet/runtimeclasses",
			inputObj: runtime.Object(
				&nodev1beta1.RuntimeClassList{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "node.k8s.io/v1beta1",
						Kind:       "RuntimeClassList",
					},
					ListMeta: metav1.ListMeta{
						ResourceVersion: "12",
					},
					Items: []nodev1beta1.RuntimeClass{},
				},
			),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/node.k8s.io/v1beta1/runtimeclasses",
			resource:   "runtimeclasses",
			namespaced: false,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{},
			},
		},
		"list with status": {
			group:   "",
			version: "v1",
			key:     "kubelet/nodetest",
			inputObj: runtime.Object(
				&metav1.Status{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Status",
					},
					Status:  "Failure",
					Message: "nodetest is not exist",
					Reason:  "NotFound",
					Code:    404,
				},
			),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/node",
			resource:   "nodes",
			namespaced: false,
			cacheErr:   storage.ErrStorageNotFound,
		},
		//used to test whether custom resource list can be cached correctly
		"cache response for list crontabs": {
			group:   "stable.example.com",
			version: "v1",
			key:     "kubelet/crontabs/default",
			inputObj: runtime.Object(
				&unstructured.UnstructuredList{
					Object: map[string]interface{}{
						"apiVersion": "stable.example.com/v1",
						"kind":       "CronTabList",
						"metadata": map[string]interface{}{
							"continue":        "",
							"resourceVersion": "2",
							"selfLink":        "/apis/stable.example.com/v1/namespaces/default/crontabs",
						},
					},
					Items: []unstructured.Unstructured{
						{
							Object: map[string]interface{}{
								"apiVersion": "stable.example.com/v1",
								"kind":       "CronTab",
								"metadata": map[string]interface{}{
									"name":            "crontab1",
									"namespace":       "default",
									"resourceVersion": "1",
								},
							},
						},
						{
							Object: map[string]interface{}{
								"apiVersion": "stable.example.com/v1",
								"kind":       "CronTab",
								"metadata": map[string]interface{}{
									"name":            "crontab2",
									"namespace":       "default",
									"resourceVersion": "2",
								},
							},
						},
					},
				},
			),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs",
			resource:   "crontabs",
			namespaced: true,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"crontab-default-crontab1-1": {},
					"crontab-default-crontab2-2": {},
				},
			},
		},
		"cache response for list foos without namespace": {
			group:   "samplecontroller.k8s.io",
			version: "v1",
			key:     "kubelet/foos",
			inputObj: runtime.Object(
				&unstructured.UnstructuredList{
					Object: map[string]interface{}{
						"apiVersion": "samplecontroller.k8s.io/v1",
						"kind":       "FooList",
						"metadata": map[string]interface{}{
							"continue":        "",
							"resourceVersion": "2",
							"selfLink":        "/apis/samplecontroller.k8s.io/v1/foos",
						},
					},
					Items: []unstructured.Unstructured{
						{
							Object: map[string]interface{}{
								"apiVersion": "samplecontroller.k8s.io/v1",
								"kind":       "Foo",
								"metadata": map[string]interface{}{
									"name":            "foo1",
									"resourceVersion": "1",
								},
							},
						},
						{
							Object: map[string]interface{}{
								"apiVersion": "samplecontroller.k8s.io/v1",
								"kind":       "Foo",
								"metadata": map[string]interface{}{
									"name":            "foo2",
									"resourceVersion": "2",
								},
							},
						},
					},
				},
			),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/samplecontroller.k8s.io/v1/foos",
			resource:   "foos",
			namespaced: false,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{
					"foo-foo1-1": {},
					"foo-foo2-2": {},
				},
			},
		},
		"list foos with no objects": {
			group:   "samplecontroller.k8s.io",
			version: "v1",
			key:     "kubelet/foos",
			inputObj: runtime.Object(
				&unstructured.UnstructuredList{
					Object: map[string]interface{}{
						"apiVersion": "samplecontroller.k8s.io/v1",
						"kind":       "FooList",
						"metadata": map[string]interface{}{
							"continue":        "",
							"resourceVersion": "2",
							"selfLink":        "/apis/samplecontroller.k8s.io/v1/foos",
						},
					},
					Items: []unstructured.Unstructured{},
				},
			),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/samplecontroller.k8s.io/v1/foos",
			resource:   "foos",
			namespaced: false,
			expectResult: struct {
				err  bool
				data map[string]struct{}
			}{
				data: map[string]struct{}{},
			},
		},
	}

	resolver := newTestRequestInfoResolver()
	for k, tt := range testcases {
		t.Run(k, func(t *testing.T) {
			s := serializerM.CreateSerializer(tt.accept, tt.group, tt.version, tt.resource)
			encoder, err := s.Encoder(tt.accept, nil)
			if err != nil {
				t.Fatalf("could not create encoder, %v", err)
			}

			buf := bytes.NewBuffer([]byte{})
			err = encoder.Encode(tt.inputObj, buf)
			if err != nil {
				t.Fatalf("could not encode input object, %v", err)
			}

			req, _ := http.NewRequest(tt.verb, tt.path, nil)
			if len(tt.userAgent) != 0 {
				req.Header.Set("User-Agent", tt.userAgent)
			}

			if len(tt.accept) != 0 {
				req.Header.Set("Accept", tt.accept)
			}
			req.RemoteAddr = "127.0.0.1"

			var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				ctx := req.Context()
				ctx = util.WithRespContentType(ctx, tt.accept)
				req = req.WithContext(ctx)
				prc := ioutil.NopCloser(buf)
				err = yurtCM.CacheResponse(req, prc, nil)
			})

			handler = proxyutil.WithRequestContentType(handler)
			handler = proxyutil.WithRequestClientComponent(handler)
			handler = filters.WithRequestInfo(handler, resolver)
			handler.ServeHTTP(httptest.NewRecorder(), req)

			if tt.expectResult.err {
				if err == nil {
					t.Error("Got no error, but expect err")
				}
			} else {
				if err != nil {
					t.Errorf("Got error %v", err)
				}

				objs, err := sWrapper.List(tt.key)
				if len(objs) == 0 {
					originGVR := schema.GroupVersionResource{
						Group:    tt.group,
						Version:  tt.version,
						Resource: tt.resource,
					}
					if isScheme, gvk := yurtCM.restMapperManager.KindFor(originGVR); !isScheme {
						if gvk.Empty() {
							t.Errorf("%s, is not stored in the DynamicRESTMapper",
								originGVR.String())
						}
					}
				}

				if err != nil {
					if err != tt.cacheErr {
						t.Errorf("expect error %v, but got %v", tt.cacheErr, err)
					}
				}

				if !compareObjectsAndKeys(t, objs, tt.namespaced, tt.expectResult.data) {
					t.Errorf("got unexpected objects for keys")
				}
			}
			err = sWrapper.DeleteCollection("kubelet")
			if err != nil {
				t.Errorf("failed to delete collection: kubelet, %v", err)
			}
			ds, _ := disk.NewDiskStorage(rootDir)
			if err = resetRESTMapper(ds, yurtCM.restMapperManager); err != nil {
				t.Errorf("failed to delete cached DynamicRESTMapper, %v", err)
			}
		})
	}
}

func TestQueryCacheForGet(t *testing.T) {
	dStorage, err := disk.NewDiskStorage(rootDir)
	if err != nil {
		t.Errorf("failed to create disk storage, %v", err)
	}
	sWrapper := NewStorageWrapper(dStorage)
	serializerM := serializer.NewSerializerManager()
	restRESTMapperMgr := meta2.NewRESTMapperManager(rootDir)
	yurtCM := &cacheManager{
		storage:           sWrapper,
		serializerManager: serializerM,
		cacheAgents:       make(map[string]bool),
		restMapperManager: restRESTMapperMgr,
	}

	testcases := map[string]struct {
		key          string
		inputObj     runtime.Object
		userAgent    string
		accept       string
		verb         string
		path         string
		namespaced   bool
		expectResult struct {
			err  bool
			rv   string
			name string
			ns   string
			kind string
		}
	}{
		"no client": {
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods/mypod1",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				err: true,
			},
		},
		"not resource request": {
			accept:     "application/json",
			verb:       "GET",
			path:       "/healthz",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				err: true,
			},
		},
		"post pod": {
			key: "kubelet/pods/default/mypod1",
			inputObj: runtime.Object(&v1.Pod{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Pod",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mypod1",
					Namespace:       "default",
					ResourceVersion: "1",
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "POST",
			path:       "/api/v1/namespaces/default/pods",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				err: true,
			},
		},
		"get pod": {
			key: "kubelet/pods/default/mypod1",
			inputObj: runtime.Object(&v1.Pod{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Pod",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mypod1",
					Namespace:       "default",
					ResourceVersion: "1",
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods/mypod1",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "1",
				name: "mypod1",
				ns:   "default",
				kind: "Pod",
			},
		},
		"update pod": {
			key: "kubelet/pods/default/mypod2",
			inputObj: runtime.Object(&v1.Pod{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Pod",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mypod2",
					Namespace:       "default",
					ResourceVersion: "2",
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "PUT",
			path:       "/api/v1/namespaces/default/pods/mypod2",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "2",
				name: "mypod2",
				ns:   "default",
				kind: "Pod",
			},
		},
		"update node": {
			key: "kubelet/nodes/mynode1",
			inputObj: runtime.Object(&v1.Node{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Node",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mynode1",
					ResourceVersion: "3",
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "PUT",
			path:       "/api/v1/nodes/mynode1",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "3",
				name: "mynode1",
				kind: "Node",
			},
		},
		"patch node": {
			key: "kubelet/nodes/mynode2",
			inputObj: runtime.Object(&v1.Node{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Node",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "mynode2",
					ResourceVersion: "4",
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "PATCH",
			path:       "/api/v1/nodes/mynode2/status",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "4",
				name: "mynode2",
				kind: "Node",
			},
		},

		//used to test whether the query local Custom Resource request can be handled correctly
		"no client for crontab": {
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs/crontab1",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				err: true,
			},
		},
		"query post crontab": {
			key: "kubelet/crontabs/default/crontab1",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "stable.example.com/v1",
					"kind":       "CronTab",
					"metadata": map[string]interface{}{
						"name":            "crontab1",
						"namespace":       "default",
						"resourceVersion": "1",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "POST",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs/crontab1",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				err: true,
			},
		},
		"query get crontab": {
			key: "kubelet/crontabs/default/crontab1",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "stable.example.com/v1",
					"kind":       "CronTab",
					"metadata": map[string]interface{}{
						"name":            "crontab1",
						"namespace":       "default",
						"resourceVersion": "1",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs/crontab1",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "1",
				name: "crontab1",
				ns:   "default",
				kind: "CronTab",
			},
		},
		"query update crontab": {
			key: "kubelet/crontabs/default/crontab2",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "stable.example.com/v1",
					"kind":       "CronTab",
					"metadata": map[string]interface{}{
						"name":            "crontab2",
						"namespace":       "default",
						"resourceVersion": "2",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "PUT",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs/crontab2",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "2",
				name: "crontab2",
				ns:   "default",
				kind: "CronTab",
			},
		},
		"query patch crontab": {
			key: "kubelet/crontabs/default/crontab3",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "stable.example.com/v1",
					"kind":       "CronTab",
					"metadata": map[string]interface{}{
						"name":            "crontab3",
						"namespace":       "default",
						"resourceVersion": "4",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "PATCH",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs/crontab3/status",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "4",
				name: "crontab3",
				ns:   "default",
				kind: "CronTab",
			},
		},
		"query post foo": {
			key: "kubelet/foos/foo1",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "samplecontroller.k8s.io/v1",
					"kind":       "Foo",
					"metadata": map[string]interface{}{
						"name":            "foo1",
						"resourceVersion": "1",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "POST",
			path:       "/apis/samplecontroller.k8s.io/v1/foos/foo1",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				err: true,
			},
		},
		"query get foo": {
			key: "kubelet/foos/foo1",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "samplecontroller.k8s.io/v1",
					"kind":       "Foo",
					"metadata": map[string]interface{}{
						"name":            "foo1",
						"resourceVersion": "1",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/samplecontroller.k8s.io/v1/foos/foo1",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "1",
				name: "foo1",
				kind: "Foo",
			},
		},
		"query update foo": {
			key: "kubelet/foos/foo2",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "samplecontroller.k8s.io/v1",
					"kind":       "Foo",
					"metadata": map[string]interface{}{
						"name":            "foo2",
						"resourceVersion": "2",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "PUT",
			path:       "/apis/samplecontroller.k8s.io/v1/foos/foo2",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "2",
				name: "foo2",
				kind: "Foo",
			},
		},
		"query patch foo": {
			key: "kubelet/foos/foo3",
			inputObj: runtime.Object(&unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "samplecontroller.k8s.io/v1",
					"kind":       "Foo",
					"metadata": map[string]interface{}{
						"name":            "foo3",
						"resourceVersion": "4",
					},
				},
			}),
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "PATCH",
			path:       "/apis/samplecontroller.k8s.io/v1/foos/foo3/status",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				name string
				ns   string
				kind string
			}{
				rv:   "4",
				name: "foo3",
				kind: "Foo",
			},
		},
	}

	accessor := meta.NewAccessor()
	resolver := newTestRequestInfoResolver()
	for k, tt := range testcases {
		t.Run(k, func(t *testing.T) {
			_ = sWrapper.Create(tt.key, tt.inputObj)
			req, _ := http.NewRequest(tt.verb, tt.path, nil)
			if len(tt.userAgent) != 0 {
				req.Header.Set("User-Agent", tt.userAgent)
			}

			if len(tt.accept) != 0 {
				req.Header.Set("Accept", tt.accept)
			}

			req.RemoteAddr = "127.0.0.1"

			var name, ns, rv, kind string
			var err error
			var obj runtime.Object
			var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				obj, err = yurtCM.QueryCache(req)
				if err == nil {
					name, _ = accessor.Name(obj)
					rv, _ = accessor.ResourceVersion(obj)
					ns, _ = accessor.Namespace(obj)
					kind, _ = accessor.Kind(obj)
				}
			})

			handler = proxyutil.WithRequestClientComponent(handler)
			handler = filters.WithRequestInfo(handler, resolver)
			handler.ServeHTTP(httptest.NewRecorder(), req)

			if tt.expectResult.err {
				if err == nil {
					t.Errorf("Got no error, but expect err")
				}
			} else {
				if err != nil {
					t.Errorf("Got error %v", err)
				}

				if tt.expectResult.name != name {
					t.Errorf("Got name %s, but expect name %s", name, tt.expectResult.name)
				}

				if tt.expectResult.rv != rv {
					t.Errorf("Got rv %s, but expect rv %s", rv, tt.expectResult.rv)
				}

				if tt.namespaced {
					if tt.expectResult.ns != ns {
						t.Errorf("Got ns %s, but expect ns %s", ns, tt.expectResult.ns)
					}
				}

				if tt.expectResult.kind != kind {
					t.Errorf("Got kind %s, but expect kind %s", kind, tt.expectResult.kind)
				}
			}

			err = sWrapper.DeleteCollection("kubelet")
			if err != nil {
				t.Errorf("failed to delete collection: kubelet, %v", err)
			}
		})
	}
}

func TestQueryCacheForList(t *testing.T) {
	dStorage, err := disk.NewDiskStorage(rootDir)
	if err != nil {
		t.Errorf("failed to create disk storage, %v", err)
	}
	sWrapper := NewStorageWrapper(dStorage)
	serializerM := serializer.NewSerializerManager()
	restRESTMapperMgr := meta2.NewRESTMapperManager(rootDir)
	yurtCM := &cacheManager{
		storage:           sWrapper,
		serializerManager: serializerM,
		cacheAgents:       make(map[string]bool),
		restMapperManager: restRESTMapperMgr,
	}

	testcases := map[string]struct {
		keyPrefix    string
		noObjs       bool
		cachedKind   string
		inputObj     []runtime.Object
		userAgent    string
		accept       string
		verb         string
		path         string
		namespaced   bool
		expectResult struct {
			err  bool
			rv   string
			data map[string]struct{}
		}
		queryErr error
	}{
		"list with no user agent": {
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				data map[string]struct{}
			}{
				err: true,
			},
		},
		"list pods": {
			keyPrefix: "kubelet/pods/default",
			inputObj: []runtime.Object{
				&v1.Pod{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Pod",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:            "mypod1",
						Namespace:       "default",
						ResourceVersion: "1",
					},
				},
				&v1.Pod{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Pod",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:            "mypod2",
						Namespace:       "default",
						ResourceVersion: "2",
					},
				},
				&v1.Pod{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Pod",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:            "mypod3",
						Namespace:       "default",
						ResourceVersion: "5",
					},
				},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/namespaces/default/pods",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				data map[string]struct{}
			}{
				rv: "5",
				data: map[string]struct{}{
					"pod-default-mypod1-1": {},
					"pod-default-mypod2-2": {},
					"pod-default-mypod3-5": {},
				},
			},
		},
		"list nodes": {
			keyPrefix: "kubelet/nodes",
			inputObj: []runtime.Object{
				&v1.Node{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Node",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:            "mynode1",
						ResourceVersion: "6",
					},
				},
				&v1.Node{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Node",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:            "mynode2",
						ResourceVersion: "8",
					},
				},
				&v1.Node{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Node",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:            "mynode3",
						ResourceVersion: "10",
					},
				},
				&v1.Node{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Node",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:            "mynode4",
						ResourceVersion: "12",
					},
				},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/nodes",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				data map[string]struct{}
			}{
				rv: "12",
				data: map[string]struct{}{
					"node-mynode1-6":  {},
					"node-mynode2-8":  {},
					"node-mynode3-10": {},
					"node-mynode4-12": {},
				},
			},
		},
		"list runtimeclass": {
			keyPrefix:  "kubelet/runtimeclasses",
			noObjs:     true,
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/node.k8s.io/v1beta1/runtimeclasses",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				data map[string]struct{}
			}{
				data: map[string]struct{}{},
			},
		},
		"list pods and no pods in cache": {
			keyPrefix:  "kubelet/pods",
			noObjs:     true,
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/pods",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				data map[string]struct{}
			}{
				err: true,
			},
			queryErr: storage.ErrStorageNotFound,
		},

		//used to test whether the query local Custom Resource list request can be handled correctly
		"list crontabs": {
			keyPrefix: "kubelet/crontabs/default",
			inputObj: []runtime.Object{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "stable.example.com/v1",
						"kind":       "CronTab",
						"metadata": map[string]interface{}{
							"name":            "crontab1",
							"namespace":       "default",
							"resourceVersion": "1",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "stable.example.com/v1",
						"kind":       "CronTab",
						"metadata": map[string]interface{}{
							"name":            "crontab2",
							"namespace":       "default",
							"resourceVersion": "2",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "stable.example.com/v1",
						"kind":       "CronTab",
						"metadata": map[string]interface{}{
							"name":            "crontab3",
							"namespace":       "default",
							"resourceVersion": "5",
						},
					},
				},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/stable.example.com/v1/namespaces/default/crontabs",
			namespaced: true,
			expectResult: struct {
				err  bool
				rv   string
				data map[string]struct{}
			}{
				rv: "5",
				data: map[string]struct{}{
					"crontab-default-crontab1-1": {},
					"crontab-default-crontab2-2": {},
					"crontab-default-crontab3-5": {},
				},
			},
		},
		"list foos": {
			keyPrefix: "kubelet/foos",
			inputObj: []runtime.Object{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "samplecontroller.k8s.io/v1",
						"kind":       "Foo",
						"metadata": map[string]interface{}{
							"name":            "foo1",
							"resourceVersion": "1",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "samplecontroller.k8s.io/v1",
						"kind":       "Foo",
						"metadata": map[string]interface{}{
							"name":            "foo2",
							"resourceVersion": "2",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "samplecontroller.k8s.io/v1",
						"kind":       "Foo",
						"metadata": map[string]interface{}{
							"name":            "foo3",
							"resourceVersion": "5",
						},
					},
				},
			},
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/samplecontroller.k8s.io/v1/foos",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				data map[string]struct{}
			}{
				rv: "5",
				data: map[string]struct{}{
					"foo-foo1-1": {},
					"foo-foo2-2": {},
					"foo-foo3-5": {},
				},
			},
		},
		"list foos with no objs": {
			keyPrefix:  "kubelet/foos",
			noObjs:     true,
			cachedKind: "samplecontroller.k8s.io/v1/Foo",
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/apis/samplecontroller.k8s.io/v1/foos",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				data map[string]struct{}
			}{
				data: map[string]struct{}{},
			},
		},
		"list resources not exist": {
			userAgent:  "kubelet",
			accept:     "application/json",
			verb:       "GET",
			path:       "/api/v1/nodes",
			namespaced: false,
			expectResult: struct {
				err  bool
				rv   string
				data map[string]struct{}
			}{
				err: true,
			},
			queryErr: storage.ErrStorageNotFound,
		},
	}

	accessor := meta.NewAccessor()
	resolver := newTestRequestInfoResolver()
	for k, tt := range testcases {
		t.Run(k, func(t *testing.T) {
			for i := range tt.inputObj {
				v, _ := accessor.Name(tt.inputObj[i])
				key := filepath.Join(tt.keyPrefix, v)
				_ = sWrapper.Create(key, tt.inputObj[i])
			}

			if tt.noObjs {
				_ = sWrapper.Create(tt.keyPrefix, nil)
				// It is used to simulate caching GVK information. If the caching is successful,
				// the next process can obtain the correct GVK information when constructing an empty List.
				if tt.cachedKind != "" {
					info := strings.Split(tt.cachedKind, meta2.SepForGVR)
					gvk := schema.GroupVersionKind{
						Group:   info[0],
						Version: info[1],
						Kind:    info[2],
					}
					_ = yurtCM.restMapperManager.UpdateKind(gvk)
				}
			}

			req, _ := http.NewRequest(tt.verb, tt.path, nil)
			if len(tt.userAgent) != 0 {
				req.Header.Set("User-Agent", tt.userAgent)
			}

			if len(tt.accept) != 0 {
				req.Header.Set("Accept", tt.accept)
			}

			req.RemoteAddr = "127.0.0.1"

			items := make([]runtime.Object, 0)
			var rv string
			var err error
			var list runtime.Object
			var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				list, err = yurtCM.QueryCache(req)
				if err == nil {
					listMetaInterface, _ := meta.ListAccessor(list)
					rv = listMetaInterface.GetResourceVersion()
					items, _ = meta.ExtractList(list)
				}
			})

			handler = proxyutil.WithRequestClientComponent(handler)
			handler = filters.WithRequestInfo(handler, resolver)
			handler.ServeHTTP(httptest.NewRecorder(), req)

			if tt.expectResult.err {
				if err == nil {
					t.Errorf("Got no error, but expect err")
				}

				if tt.queryErr != nil && tt.queryErr != err {
					t.Errorf("expect err %v, but got %v", tt.queryErr, err)
				}
			} else {
				if err != nil {
					t.Errorf("Got error %v", err)
				}

				if tt.expectResult.rv != "" && tt.expectResult.rv != rv {
					t.Errorf("Got rv %s, but expect rv %s", rv, tt.expectResult.rv)
				}

				if !compareObjectsAndKeys(t, items, tt.namespaced, tt.expectResult.data) {
					t.Errorf("got unexpected objects for keys")
				}
			}
			err = sWrapper.DeleteCollection("kubelet")
			if err != nil {
				t.Errorf("failed to delete collection: kubelet, %v", err)
			}
			ds, _ := disk.NewDiskStorage(rootDir)
			if err = resetRESTMapper(ds, yurtCM.restMapperManager); err != nil {
				t.Errorf("failed to delete cached DynamicRESTMapper, %v", err)
			}
		})
	}
}

func compareObjectsAndKeys(t *testing.T, objs []runtime.Object, namespaced bool, keys map[string]struct{}) bool {
	if len(objs) != len(keys) {
		t.Errorf("expect %d keys, but got %d objects", len(keys), len(objs))
		return false
	}

	accessor := meta.NewAccessor()
	objKeys := make(map[string]struct{})
	for i := range objs {
		kind, _ := accessor.Kind(objs[i])
		ns, _ := accessor.Namespace(objs[i])
		name, _ := accessor.Name(objs[i])
		itemRv, _ := accessor.ResourceVersion(objs[i])

		if namespaced {
			objKeys[fmt.Sprintf("%s-%s-%s-%s", strings.ToLower(kind), ns, name, itemRv)] = struct{}{}
		} else {
			objKeys[fmt.Sprintf("%s-%s-%s", strings.ToLower(kind), name, itemRv)] = struct{}{}
		}
	}

	if len(objKeys) != len(keys) {
		t.Errorf("expect %d keys, but got %d object keys", len(keys), len(objKeys))
		return false
	}

	for key := range objKeys {
		if _, ok := keys[key]; !ok {
			t.Errorf("got unexpected object with key: %s", key)
			return false
		}
	}

	return true
}

//used to reset the RESTMapper
func resetRESTMapper(dStorage storage.Store, rm *meta2.RESTMapperManager) error {
	b, err := dStorage.Get(meta2.CacheDynamicRESTMapperKey)
	if err == storage.ErrStorageNotFound {
		return nil
	} else if err != nil {
		return err
	}
	cacheMapper := make(map[string]string)
	if err = json.Unmarshal(b, &cacheMapper); err != nil {
		return err
	}
	for gvrString := range cacheMapper {
		localInfo := strings.Split(gvrString, meta2.SepForGVR)
		if len(localInfo) != 3 {
			return fmt.Errorf("this %s is not standardized", gvrString)
		}
		gvr := schema.GroupVersionResource{
			Group:    localInfo[0],
			Version:  localInfo[1],
			Resource: localInfo[2],
		}
		if err = rm.DeleteKindFor(gvr); err != nil {
			return err
		}
	}
	err = dStorage.DeleteCollection("_internal")
	if err != nil {
		return err
	}
	return nil
}

func TestCanCacheFor(t *testing.T) {
	dStorage, err := disk.NewDiskStorage(rootDir)
	if err != nil {
		t.Errorf("failed to create disk storage, %v", err)
	}
	s := NewStorageWrapper(dStorage)
	m, _ := NewCacheManager(s, nil, nil)

	type proxyRequest struct {
		userAgent string
		verb      string
		path      string
		header    map[string]string
	}

	testcases := map[string]struct {
		preRequest     *proxyRequest
		preExpectCache bool
		request        *proxyRequest
		expectCache    bool
	}{
		"no user agent": {
			request: &proxyRequest{
				verb: "GET",
				path: "/api/v1/nodes/mynode",
			},
			expectCache: false,
		},
		"not default user agent": {
			request: &proxyRequest{
				userAgent: "kubelet-test",
				verb:      "GET",
				path:      "/api/v1/nodes/mynode",
			},
			expectCache: false,
		},
		"default user agent kubelet": {
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/nodes/mynode",
			},
			expectCache: true,
		},
		"default user agent flanneld": {
			request: &proxyRequest{
				userAgent: "flanneld",
				verb:      "POST",
				path:      "/api/v1/nodes/mynode",
			},
			expectCache: true,
		},
		"default user agent coredns": {
			request: &proxyRequest{
				userAgent: "coredns",
				verb:      "PUT",
				path:      "/api/v1/nodes/mynode",
			},
			expectCache: true,
		},
		"default user agent kube-proxy": {
			request: &proxyRequest{
				userAgent: "kube-proxy",
				verb:      "PATCH",
				path:      "/api/v1/nodes/mynode",
			},
			expectCache: true,
		},
		"default user agent tunnel-agent": {
			request: &proxyRequest{
				userAgent: projectinfo.GetAgentName(),
				verb:      "HEAD",
				path:      "/api/v1/nodes/mynode",
			},
			expectCache: true,
		},
		"with cache header": {
			request: &proxyRequest{
				userAgent: "test1",
				verb:      "GET",
				path:      "/api/v1/nodes/mynode",
				header:    map[string]string{"Edge-Cache": "true"},
			},
			expectCache: true,
		},
		"with cache header false": {
			request: &proxyRequest{
				userAgent: "test2",
				verb:      "GET",
				path:      "/api/v1/nodes/mynode",
				header:    map[string]string{"Edge-Cache": "false"},
			},
			expectCache: false,
		},
		"not resource request": {
			request: &proxyRequest{
				userAgent: "test2",
				verb:      "GET",
				path:      "/healthz",
				header:    map[string]string{"Edge-Cache": "true"},
			},
			expectCache: false,
		},
		"delete request": {
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "DELETE",
				path:      "/api/v1/nodes/mynode",
			},
			expectCache: false,
		},
		"delete collection request": {
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "DELETE",
				path:      "/api/v1/namespaces/default/pods",
			},
			expectCache: false,
		},
		"proxy request": {
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/proxy/namespaces/default/pods/test",
			},
			expectCache: false,
		},
		"get status sub resource request": {
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/default/pods/test/status",
			},
			expectCache: true,
		},
		"get not status sub resource request": {
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/default/pods/test/proxy",
			},
			expectCache: false,
		},
		"list requests with no selectors": {
			preRequest: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/default/pods",
			},
			preExpectCache: true,
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/default/pods",
			},
			expectCache: true,
		},
		"list requests with label selectors": {
			preRequest: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/kube-system/pods?labelSelector=foo=bar",
			},
			preExpectCache: true,
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/kube-system/pods?labelSelector=foo=bar",
			},
			expectCache: true,
		},
		"list requests with field selectors": {
			preRequest: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/test2/pods?fieldSelector=spec.nodeName=test",
			},
			preExpectCache: true,
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/test2/pods?fieldSelector=spec.nodeName=test",
			},
			expectCache: true,
		},
		"list requests have same path but with different selectors": {
			preRequest: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/test2/secrets?labelSelector=foo=bar1",
			},
			preExpectCache: true,
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/test2/secrets?labelSelector=foo=bar2",
			},
			expectCache: false,
		},
		"list requests get same resouces but with different path": {
			preRequest: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/namespaces/test2/configmaps?labelSelector=foo=bar1",
			},
			preExpectCache: true,
			request: &proxyRequest{
				userAgent: "kubelet",
				verb:      "GET",
				path:      "/api/v1/configmaps?labelSelector=foo=bar2",
			},
			expectCache: false,
		},
	}

	for k, tt := range testcases {
		t.Run(k, func(t *testing.T) {
			if tt.preRequest != nil {
				reqCanCache := checkReqCanCache(m, tt.preRequest.userAgent, tt.preRequest.verb, tt.preRequest.path, tt.preRequest.header)
				if reqCanCache != tt.preExpectCache {
					t.Errorf("Got request pre can cache %v, but expect request pre can cache %v", reqCanCache, tt.preExpectCache)
				}
			}

			if tt.request != nil {
				reqCanCache := checkReqCanCache(m, tt.request.userAgent, tt.request.verb, tt.request.path, tt.request.header)
				if reqCanCache != tt.expectCache {
					t.Errorf("Got request can cache %v, but expect request can cache %v", reqCanCache, tt.expectCache)
				}
			}
		})
	}
}

func checkReqCanCache(m CacheManager, userAgent, verb, path string, header map[string]string) bool {
	req, _ := http.NewRequest(verb, path, nil)
	if len(userAgent) != 0 {
		req.Header.Set("User-Agent", userAgent)
	}

	for k, v := range header {
		req.Header.Set(k, v)
	}

	req.RemoteAddr = "127.0.0.1"

	var reqCanCache bool
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		reqCanCache = m.CanCacheFor(req)

	})

	handler = proxyutil.WithListRequestSelector(handler)
	handler = proxyutil.WithCacheHeaderCheck(handler)
	handler = proxyutil.WithRequestClientComponent(handler)
	handler = filters.WithRequestInfo(handler, newTestRequestInfoResolver())
	handler.ServeHTTP(httptest.NewRecorder(), req)

	return reqCanCache
}
