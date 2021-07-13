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

package disk

import (
	"os"
	"testing"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

var (
	testDir = "/tmp/cache/"
)

func TestCreate(t *testing.T) {
	testcases := map[string]struct {
		rootDir       string
		preCreatedKey map[string]string
		keysData      map[string]string
		result        map[string]struct {
			err  error
			data string
		}
		createErr error
	}{
		"create dir key with no data": {
			keysData: map[string]string{
				"/kubelet/default/pods": "",
			},
			result: map[string]struct {
				err  error
				data string
			}{
				"/kubelet/default/pods": {
					err: storage.ErrKeyHasNoContent,
				},
			},
		},
		"create dir key that already exists": {
			preCreatedKey: map[string]string{
				"/kubelet/default/pods/foo": "test-pod1",
			},
			keysData: map[string]string{
				"/kubelet/default/pods": "",
			},
			result: map[string]struct {
				err  error
				data string
			}{
				"/kubelet/default/pods/foo": {
					storage.ErrStorageNotFound,
					"",
				},
			},
		},
		"create key normally": {
			keysData: map[string]string{
				"/kubelet/default/pods/foo": "test-pod",
			},
			result: map[string]struct {
				err  error
				data string
			}{
				"/kubelet/default/pods/foo": {
					data: "test-pod",
				},
			},
		},
		"create key when key already exists": {
			preCreatedKey: map[string]string{
				"/kubelet/default/pods/bar": "test-pod1",
			},
			keysData: map[string]string{
				"/kubelet/default/pods/bar": "test-pod2",
			},
			result: map[string]struct {
				err  error
				data string
			}{
				"/kubelet/default/pods/bar": {
					data: "test-pod2",
				},
			},
		},
		"create key when parent dir already exists": {
			preCreatedKey: map[string]string{
				"/kubelet/default/pods": "",
			},
			keysData: map[string]string{
				"/kubelet/default/pods/bar": "test-pod1",
			},
			result: map[string]struct {
				err  error
				data string
			}{
				"/kubelet/default/pods/bar": {
					data: "test-pod1",
				},
			},
		},
		"create key that already exists with no data": {
			preCreatedKey: map[string]string{
				"/kubelet/default/pods/foo": "test-pod1",
			},
			keysData: map[string]string{
				"/kubelet/default/pods/foo": "",
			},
			createErr: storage.ErrKeyHasNoContent,
		},
	}
	s, err := NewDiskStorage(testDir)
	if err != nil {
		t.Fatalf("unable to new disk storage, %v", err)
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			for key, data := range tc.preCreatedKey {
				err = s.Create(key, []byte(data))
				if err != nil {
					t.Errorf("%s Got error %v, wanted successful create %s", k, err, key)
				}
			}

			for key, data := range tc.keysData {
				err = s.Create(key, []byte(data))
				if err != nil {
					if tc.createErr != err {
						t.Errorf("%s: expect create error %v, but got %v", k, tc.createErr, err)
					}
				}
			}

			for key, result := range tc.result {
				b, err := s.Get(key)
				if result.err != nil {
					if result.err != err {
						t.Errorf("%s(key=%s) expect error %v, but got error %v", k, key, result.err, err)
					}
				}

				if result.data != "" {
					if result.data != string(b) {
						t.Errorf("%s(key=%s) expect result %s, but got data %s", k, key, result.data, string(b))
					}
				}
			}

			for key := range tc.result {
				if err = s.Delete(key); err != nil {
					t.Errorf("%s failed to delete key %s, %v", k, key, err)
				}
			}
		})
	}

	if err = os.RemoveAll(testDir); err != nil {
		t.Errorf("Got error %v, unable remove path %s", err, testDir)
	}
}

func TestDelete(t *testing.T) {
	testcases := map[string]struct {
		preCreatedKeys map[string]string
		deleteKeys     []string
		result         map[string]struct {
			beforeDelete string
			deleteErr    error
			getErr       error
		}
	}{
		"normally delete key": {
			preCreatedKeys: map[string]string{
				"kubelet/nodes/foo": "node-test1",
			},
			deleteKeys: []string{"kubelet/nodes/foo"},
			result: map[string]struct {
				beforeDelete string
				deleteErr    error
				getErr       error
			}{
				"kubelet/nodes/foo": {
					beforeDelete: "node-test1",
					deleteErr:    nil,
					getErr:       storage.ErrStorageNotFound,
				},
			},
		},
		"delete key that not exist": {
			deleteKeys: []string{"kubelet/nodes/foo"},
			result: map[string]struct {
				beforeDelete string
				deleteErr    error
				getErr       error
			}{
				"kubelet/nodes/foo": {
					deleteErr: nil,
				},
			},
		},
		"delete dir key": {
			preCreatedKeys: map[string]string{
				"kubelet/nodes": "",
			},
			deleteKeys: []string{"kubelet/nodes"},
			result: map[string]struct {
				beforeDelete string
				deleteErr    error
				getErr       error
			}{
				"kubelet/nodes/": {
					deleteErr: nil,
					getErr:    storage.ErrKeyHasNoContent,
				},
			},
		},
	}
	s, err := NewDiskStorage(testDir)
	if err != nil {
		t.Fatalf("unable to new disk storage, %v", err)
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			for key, data := range tc.preCreatedKeys {
				err = s.Create(key, []byte(data))
				if err != nil {
					t.Errorf("%s: Got error %v, wanted successful create %s", k, err, key)
				}
			}

			for key, result := range tc.result {
				if result.beforeDelete != "" {
					b, err := s.Get(key)
					if err != nil {
						t.Errorf("%s: got error %v when get key %s before deletion", k, err, key)
					}

					if result.beforeDelete != string(b) {
						t.Errorf("%s: expect data %s, but got %s before deletion", k, result.beforeDelete, string(b))
					}
				}
			}

			for _, key := range tc.deleteKeys {
				err = s.Delete(key)
				if result, ok := tc.result[key]; ok {
					if result.deleteErr != err {
						t.Errorf("%s: delete key(%s) expect error %v, but got %v", k, key, result.deleteErr, err)
					}
				} else if err != nil {
					t.Errorf("%s: Got error %v, unable delete key %q", k, err, key)
				}
			}

			for key, result := range tc.result {
				_, err := s.Get(key)
				if result.getErr != nil {
					if result.getErr != err {
						t.Errorf("%s: expect error %v, but got error %v", k, result.getErr, err)
					}
				}
			}
		})
	}

	if err = os.RemoveAll(testDir); err != nil {
		t.Errorf("Got error %v, unable remove path %s", err, testDir)
	}
}

func TestGet(t *testing.T) {
	testcases := map[string]struct {
		preCreatedKeys map[string]string
		result         map[string]struct {
			err  error
			data string
		}
	}{
		"normally get key": {
			preCreatedKeys: map[string]string{
				"kubelet/nodes/foo": "test-node1",
			},
			result: map[string]struct {
				err  error
				data string
			}{
				"kubelet/nodes/foo": {
					data: "test-node1",
				},
			},
		},
		"get key that is not exist": {
			result: map[string]struct {
				err  error
				data string
			}{
				"kubelet/nodes/foo": {
					err: storage.ErrStorageNotFound,
				},
			},
		},
		"get key that is not regular file": {
			preCreatedKeys: map[string]string{
				"kubelet/nodes": "",
			},
			result: map[string]struct {
				err  error
				data string
			}{
				"kubelet/nodes": {
					err: storage.ErrKeyHasNoContent,
				},
			},
		},
	}
	s, err := NewDiskStorage(testDir)
	if err != nil {
		t.Fatalf("unable to new disk storage, %v", err)
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			for key, data := range tc.preCreatedKeys {
				err := s.Create(key, []byte(data))
				if err != nil {
					t.Errorf("%s Got error %v, wanted successful create %s", k, err, key)
				}
			}

			for key, result := range tc.result {
				b, err := s.Get(key)
				if result.err != nil {
					if result.err != err {
						t.Errorf("%s: expect error %v, but got error %v", k, result.err, err)
					}
				}

				if result.data != "" {
					if result.data != string(b) {
						t.Errorf("%s: expect result data %s, but got data %s", k, result.data, string(b))
					}
				}
			}

			for key := range tc.result {
				if err = s.Delete(key); err != nil {
					t.Errorf("%s failed to delete key %s, %v", k, key, err)
				}
			}
		})
	}

	if err = os.RemoveAll(testDir); err != nil {
		t.Errorf("Got error %v, unable remove path %s", err, testDir)
	}
}

func TestListKeys(t *testing.T) {
	testcases := map[string]struct {
		preCreatedKeys map[string]string
		listKey        string
		result         map[string]struct{}
		listErr        error
	}{
		"normally list keys": {
			preCreatedKeys: map[string]string{
				"kubelet/pods/default/foo1":         "pod1",
				"kubelet/pods/default/foo2":         "pod2",
				"kubelet/pods/kube-system/foo3":     "pod3",
				"kubelet/pods/kube-system/foo4":     "pod4",
				"kubelet/pods/kube-system/tmp_foo5": "pod5",
				"kubelet/pods/foo5":                 "",
			},
			listKey: "kubelet/pods",
			result: map[string]struct{}{
				"kubelet/pods/default/foo1":     {},
				"kubelet/pods/default/foo2":     {},
				"kubelet/pods/kube-system/foo3": {},
				"kubelet/pods/kube-system/foo4": {},
			},
		},
		"list keys for dir only": {
			preCreatedKeys: map[string]string{
				"kubelet/pods/default":     "",
				"kubelet/pods/kube-system": "",
				"kubelet/pods/foo5":        "",
			},
			listKey: "kubelet/pods",
			result:  map[string]struct{}{},
		},
		"list keys for regular file": {
			preCreatedKeys: map[string]string{
				"kubelet/pods/default/foo1": "pod1",
			},
			listKey: "kubelet/pods/default/foo1",
			result: map[string]struct{}{
				"kubelet/pods/default/foo1": {},
			},
		},
		"list keys for not exist file": {
			listKey: "kubelet/pods/default/foo5",
			result:  map[string]struct{}{},
		},
	}
	s, err := NewDiskStorage(testDir)
	if err != nil {
		t.Fatalf("unable to new disk storage, %v", err)
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			for key, data := range tc.preCreatedKeys {
				err = s.Create(key, []byte(data))
				if err != nil {
					t.Errorf("%s: Got error %v, wanted successful create %s", k, err, key)
				}
			}

			keys, err := s.ListKeys(tc.listKey)
			if err != nil {
				t.Errorf("%s: Got error %v, unable list keys for %s", k, err, tc.listKey)
			}

			if len(tc.result) != len(keys) {
				t.Errorf("%s: expect %d keys, but got %d keys", k, len(tc.result), len(keys))
			}

			for _, key := range keys {
				if _, ok := tc.result[key]; !ok {
					t.Errorf("%s: got key %s not in result %v", k, key, tc.result)
				}
			}

			for key := range tc.preCreatedKeys {
				err = s.Delete(key)
				if err != nil {
					t.Errorf("%s: failed to delete key(%s), %v", k, key, err)
				}
			}
		})
	}

	if err = os.RemoveAll(testDir); err != nil {
		t.Errorf("Got error %v, unable remove path %s", err, testDir)
	}
}

func TestList(t *testing.T) {
	testcases := map[string]struct {
		preCreatedKeys map[string]string
		listKey        string
		result         map[string]struct{}
		listErr        error
	}{
		"normally list": {
			preCreatedKeys: map[string]string{
				"kubelet/pods/default/foo1":         "pod1",
				"kubelet/pods/default/foo2":         "pod2",
				"kubelet/pods/kube-system/foo3":     "pod3",
				"kubelet/pods/kube-system/foo4":     "pod4",
				"kubelet/pods/kube-system/tmp_foo5": "pod5",
				"kubelet/pods/foo5":                 "",
			},
			listKey: "kubelet/pods",
			result: map[string]struct{}{
				"pod1": {},
				"pod2": {},
				"pod3": {},
				"pod4": {},
			},
		},
		"list for empty dir": {
			preCreatedKeys: map[string]string{
				"kubelet/pods/default":     "",
				"kubelet/pods/kube-system": "",
				"kubelet/pods/foo5":        "",
			},
			listKey: "kubelet/pods",
			result:  map[string]struct{}{},
		},
		"list for regular file": {
			preCreatedKeys: map[string]string{
				"kubelet/pods/default/foo1": "pod1",
			},
			listKey: "kubelet/pods/default/foo1",
			result: map[string]struct{}{
				"pod1": {},
			},
		},
		"list for not exist file": {
			listKey: "kubelet/pods/default/foo5",
			result:  map[string]struct{}{},
			listErr: storage.ErrStorageNotFound,
		},
	}
	s, err := NewDiskStorage(testDir)
	if err != nil {
		t.Fatalf("unable to new disk storage, %v", err)
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			for key, data := range tc.preCreatedKeys {
				err = s.Create(key, []byte(data))
				if err != nil {
					t.Errorf("%s: Got error %v, wanted successful create %s", k, err, key)
				}
			}

			data, err := s.List(tc.listKey)
			if err != nil {
				if tc.listErr != err {
					t.Errorf("%s: list(%s) expect error %v, but got error %v", k, tc.listKey, tc.listErr, err)
				}
			}

			if len(tc.result) != len(data) {
				t.Errorf("%s: list expect %d objects, but got %d objects", k, len(tc.result), len(data))
			}

			for i := range data {
				if _, ok := tc.result[string(data[i])]; !ok {
					t.Errorf("%s: list data %s not in result %v", k, string(data[i]), tc.result)
				}
			}

			for key := range tc.preCreatedKeys {
				err = s.Delete(key)
				if err != nil {
					t.Errorf("%s: failed to delete key(%s), %v", k, key, err)
				}
			}
		})
	}

	if err = os.RemoveAll(testDir); err != nil {
		t.Errorf("Got error %v, unable remove path %s", err, testDir)
	}
}

func TestUpdate(t *testing.T) {
	testcases := map[string]struct {
		preCreatedKeys map[string]string
		updateKeys     map[string]string
		result         map[string]string
		updateErr      error
	}{
		"normally update keys": {
			preCreatedKeys: map[string]string{
				"kubelet/pods/default/foo1": "test-pod1",
				"kubelet/pods/default/foo2": "test-pod2",
			},
			updateKeys: map[string]string{
				"kubelet/pods/default/foo1": "test-pod11",
				"kubelet/pods/default/foo2": "test-pod21",
			},
			result: map[string]string{
				"kubelet/pods/default/foo1": "test-pod11",
				"kubelet/pods/default/foo2": "test-pod21",
			},
		},
		"update keys that not exist": {
			updateKeys: map[string]string{
				"kubelet/pods/default/foo1": "test-pod11",
				"kubelet/pods/default/foo2": "test-pod21",
			},
			result: map[string]string{
				"kubelet/pods/default/foo1": "test-pod11",
				"kubelet/pods/default/foo2": "test-pod21",
			},
		},
		"update keys with empty string": {
			preCreatedKeys: map[string]string{
				"kubelet/pods/default/foo1": "test-pod1",
				"kubelet/pods/default/foo2": "test-pod2",
			},
			updateKeys: map[string]string{
				"kubelet/pods/default/foo1": "",
				"kubelet/pods/default/foo2": "",
			},
			result: map[string]string{
				"kubelet/pods/default/foo1": "test-pod1",
				"kubelet/pods/default/foo2": "test-pod2",
			},
			updateErr: storage.ErrKeyHasNoContent,
		},
	}
	s, err := NewDiskStorage(testDir)
	if err != nil {
		t.Fatalf("unable to new disk storage, %v", err)
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			for key, data := range tc.preCreatedKeys {
				err = s.Create(key, []byte(data))
				if err != nil {
					t.Errorf("%s: Got error %v, wanted successful create %s", k, err, key)
				}
			}

			for key, data := range tc.updateKeys {
				err = s.Update(key, []byte(data))
				if err != nil {
					if tc.updateErr != err {
						t.Errorf("%s: expect error %v, but got %v", k, tc.updateErr, err)
					}
				}
			}

			for key, data := range tc.result {
				b, err := s.Get(key)
				if err != nil {
					t.Errorf("%s: Got error %v, unable get key %s", k, err, key)
				}

				if data != string(b) {
					t.Errorf("%s: expect updated data %s, but got %s", k, data, string(b))
				}
			}

			for key := range testcases {
				err = s.Delete(key)
				if err != nil {
					t.Errorf("%s: failed to delete key(%s), %v", k, key, err)
				}
			}
		})
	}

	if err = os.RemoveAll(testDir); err != nil {
		t.Errorf("Got error %v, unable remove path %s", err, testDir)
	}
}

func TestLockKey(t *testing.T) {
	testcases := map[string]struct {
		preLockedKeys    []string
		lockKey          string
		lockResult       bool
		lockVerifyResult bool
	}{
		"normal lock key": {
			preLockedKeys:    []string{},
			lockKey:          "kubelet/pods/kube-system/foo",
			lockResult:       true,
			lockVerifyResult: false,
		},
		"lock pre-locked key with same key": {
			preLockedKeys:    []string{"kubelet/pods/kube-system/foo"},
			lockKey:          "kubelet/pods/kube-system/foo",
			lockResult:       false,
			lockVerifyResult: false,
		},
		"lock child key of pre-locked key": {
			preLockedKeys:    []string{"kubelet/pods/kube-system"},
			lockKey:          "kubelet/pods/kube-system/foo",
			lockResult:       false,
			lockVerifyResult: false,
		},
		"lock parent key of pre-locked key ": {
			preLockedKeys:    []string{"kubelet/pods/kube-system/foo"},
			lockKey:          "kubelet/pods/kube-system",
			lockResult:       false,
			lockVerifyResult: false,
		},
		"lock different key with pre-locked key ": {
			preLockedKeys:    []string{"kubelet/pods/kube-system/foo1"},
			lockKey:          "kubelet/pods/kube-system/foo2",
			lockResult:       true,
			lockVerifyResult: false,
		},
	}

	s := &diskStorage{
		keyPendingStatus: make(map[string]struct{}),
		baseDir:          testDir,
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			for _, key := range tc.preLockedKeys {
				s.lockKey(key)
			}

			lResult := s.lockKey(tc.lockKey)
			if lResult != tc.lockResult {
				t.Errorf("%s: expect lock result %v, but got %v", k, tc.lockResult, lResult)
			}

			lResult2 := s.lockKey(tc.lockKey)
			if lResult2 != tc.lockVerifyResult {
				t.Errorf("%s: expect lock verify result %v, but got %v", k, tc.lockVerifyResult, lResult2)
			}

			s.unLockKey(tc.lockKey)
			for _, key := range tc.preLockedKeys {
				s.unLockKey(key)
			}
		})
	}
}
