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
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage/disk"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var rootDir = "/tmp/restmapper"

func TestCreateRESTMapperManager(t *testing.T) {
	dStorage, err := disk.NewDiskStorage(rootDir)
	if err != nil {
		t.Errorf("failed to create disk storage, %v", err)
	}
	defer func() {
		if err := os.RemoveAll(rootDir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", rootDir, err)
		}
	}()

	// initialize an empty DynamicRESTMapper
	yurtHubRESTMapperManager := NewRESTMapperManager(rootDir)
	if yurtHubRESTMapperManager.dynamicRESTMapper == nil || len(yurtHubRESTMapperManager.dynamicRESTMapper) != 0 {
		t.Errorf("failed to initialize an empty dynamicRESTMapper, %v", err)
	}

	// reset yurtHubRESTMapperManager
	if err := resetRESTMapper(dStorage, yurtHubRESTMapperManager); err != nil {
		t.Fatalf("failed to reset yurtHubRESTMapperManager , %v", err)
	}

	// initialize an Non-empty DynamicRESTMapper
	// pre-cache the CRD information to the hard disk
	cachedDynamicRESTMapper := map[string]string{
		"samplecontroller.k8s.io/v1alpha1/foo":  "Foo",
		"samplecontroller.k8s.io/v1alpha1/foos": "Foo",
	}
	d, err := json.Marshal(cachedDynamicRESTMapper)
	if err != nil {
		t.Errorf("failed to serialize dynamicRESTMapper, %v", err)
	}
	err = dStorage.Update(CacheDynamicRESTMapperKey, d)
	if err != nil {
		t.Fatalf("failed to stored dynamicRESTMapper, %v", err)
	}

	// Determine whether the restmapper in the memory is the same as the information written to the disk
	yurtHubRESTMapperManager = NewRESTMapperManager(rootDir)
	// get the CRD information in memory
	m := yurtHubRESTMapperManager.dynamicRESTMapper
	gotMapper := dynamicRESTMapperToString(m)

	if !compareDynamicRESTMapper(gotMapper, cachedDynamicRESTMapper) {
		t.Errorf("Got mapper: %v, expect mapper: %v", gotMapper, cachedDynamicRESTMapper)
	}

	if err := resetRESTMapper(dStorage, yurtHubRESTMapperManager); err != nil {
		t.Fatalf("failed to reset yurtHubRESTMapperManager , %v", err)
	}
}

func TestUpdateRESTMapper(t *testing.T) {
	dStorage, err := disk.NewDiskStorage(rootDir)
	if err != nil {
		t.Errorf("failed to create disk storage, %v", err)
	}
	defer func() {
		if err := os.RemoveAll(rootDir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", rootDir, err)
		}
	}()
	yurtHubRESTMapperManager := NewRESTMapperManager(rootDir)
	testcases := map[string]struct {
		cachedCRD        []schema.GroupVersionKind
		addCRD           schema.GroupVersionKind
		deleteCRD        schema.GroupVersionResource
		expectRESTMapper map[string]string
	}{
		"add the first CRD": {
			cachedCRD: []schema.GroupVersionKind{},
			addCRD:    schema.GroupVersionKind{Group: "samplecontroller.k8s.io", Version: "v1alpha1", Kind: "Foo"},
			expectRESTMapper: map[string]string{
				"samplecontroller.k8s.io/v1alpha1/foo":  "Foo",
				"samplecontroller.k8s.io/v1alpha1/foos": "Foo",
			},
		},

		"update with another CRD": {
			cachedCRD: []schema.GroupVersionKind{{Group: "samplecontroller.k8s.io", Version: "v1alpha1", Kind: "Foo"}},
			addCRD:    schema.GroupVersionKind{Group: "stable.example.com", Version: "v1", Kind: "CronTab"},
			expectRESTMapper: map[string]string{
				"samplecontroller.k8s.io/v1alpha1/foo":  "Foo",
				"samplecontroller.k8s.io/v1alpha1/foos": "Foo",
				"stable.example.com/v1/crontab":         "CronTab",
				"stable.example.com/v1/crontabs":        "CronTab",
			},
		},
		"delete one CRD": {
			cachedCRD: []schema.GroupVersionKind{
				{Group: "samplecontroller.k8s.io", Version: "v1alpha1", Kind: "Foo"},
				{Group: "stable.example.com", Version: "v1", Kind: "CronTab"},
			},
			deleteCRD: schema.GroupVersionResource{Group: "stable.example.com", Version: "v1", Resource: "crontabs"},
			expectRESTMapper: map[string]string{
				"samplecontroller.k8s.io/v1alpha1/foo":  "Foo",
				"samplecontroller.k8s.io/v1alpha1/foos": "Foo",
			},
		},
	}
	for k, tt := range testcases {
		t.Run(k, func(t *testing.T) {
			// initialize the cache CRD
			for _, gvk := range tt.cachedCRD {
				err := yurtHubRESTMapperManager.UpdateKind(gvk)
				if err != nil {
					t.Errorf("failed to initialize the restmapper, %v", err)
				}
			}
			// add CRD information
			if !tt.addCRD.Empty() {
				err := yurtHubRESTMapperManager.UpdateKind(tt.addCRD)
				if err != nil {
					t.Errorf("failed to add CRD information, %v", err)
				}
			} else {
				// delete CRD information
				err := yurtHubRESTMapperManager.DeleteKindFor(tt.deleteCRD)
				if err != nil {
					t.Errorf("failed to delete CRD information, %v", err)
				}
			}

			// verify the CRD information in memory
			m := yurtHubRESTMapperManager.dynamicRESTMapper
			memoryMapper := dynamicRESTMapperToString(m)

			if !compareDynamicRESTMapper(memoryMapper, tt.expectRESTMapper) {
				t.Errorf("Got mapper: %v, expect mapper: %v", memoryMapper, tt.expectRESTMapper)
			}

			// verify the CRD information in disk
			b, err := dStorage.Get(CacheDynamicRESTMapperKey)
			if err != nil {
				t.Fatalf("failed to get cached CRD information, %v", err)
			}
			cacheMapper := make(map[string]string)
			err = json.Unmarshal(b, &cacheMapper)
			if err != nil {
				t.Errorf("failed to decode the cached dynamicRESTMapper, %v", err)
			}

			if !compareDynamicRESTMapper(cacheMapper, tt.expectRESTMapper) {
				t.Errorf("cached mapper: %v, expect mapper: %v", cacheMapper, tt.expectRESTMapper)
			}
		})
	}
	if err := resetRESTMapper(dStorage, yurtHubRESTMapperManager); err != nil {
		t.Fatalf("failed to reset yurtHubRESTMapperManager , %v", err)
	}
}

func compareDynamicRESTMapper(gotMapper map[string]string, expectedMapper map[string]string) bool {
	if len(gotMapper) != len(expectedMapper) {
		return false
	}

	for gvr, kind := range gotMapper {
		k, exists := expectedMapper[gvr]
		if !exists || k != kind {
			return false
		}
	}

	return true
}

func dynamicRESTMapperToString(m map[schema.GroupVersionResource]schema.GroupVersionKind) map[string]string {
	resultMapper := make(map[string]string, len(m))
	for currResource, currKind := range m {
		//key: Group/Version/Resource, value: Kind
		k := strings.Join([]string{currResource.Group, currResource.Version, currResource.Resource}, SepForGVR)
		resultMapper[k] = currKind.Kind
	}
	return resultMapper
}

func resetRESTMapper(storage storage.Store, rmm *RESTMapperManager) error {
	rmm.dynamicRESTMapper = nil
	err := storage.Delete(CacheDynamicRESTMapperKey)
	if err != nil {
		return err
	}
	return nil
}
