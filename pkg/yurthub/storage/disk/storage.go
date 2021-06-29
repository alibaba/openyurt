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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
	"k8s.io/klog"
)

const (
	CacheBaseDir = "/etc/kubernetes/cache/"
	tmpPrefix    = "tmp_"
)

type diskStorage struct {
	baseDir          string
	keyPendingStatus map[string]struct{}
	sync.Mutex
}

// NewDiskStorage creates a storage.Store for caching data into local disk
func NewDiskStorage(dir string) (storage.Store, error) {
	if dir == "" {
		klog.Infof("disk cache path is empty, set it by default %s", CacheBaseDir)
		dir = CacheBaseDir
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	ds := &diskStorage{
		keyPendingStatus: make(map[string]struct{}),
		baseDir:          dir,
	}

	err := ds.Recover("")
	if err != nil {
		klog.Errorf("could not recover local storage, %v, and skip the error", err)
	}
	return ds, nil
}

// Create new a file with key and contents or create dir only
// when contents are empty.
// Note: when the content is empty and the dir already exists, the create function will empty the current dir
func (ds *diskStorage) Create(key string, contents []byte) error {
	if key == "" {
		return storage.ErrKeyIsEmpty
	}

	if !ds.lockKey(key) {
		return storage.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)

	// no contents, create key dir only
	if len(contents) == 0 {
		keyPath := filepath.Join(ds.baseDir, key)
		if info, err := os.Stat(keyPath); err != nil {
			if os.IsNotExist(err) {
				if err = os.MkdirAll(keyPath, 0755); err == nil {
					return nil
				}
			}
			return err
		} else if info.IsDir() {
			// ensure that there are no files left locally before create an dir
			if fileInfoList, err := ioutil.ReadDir(keyPath); err != nil {
				return err
			} else if len(fileInfoList) != 0 {
				for _, f := range fileInfoList {
					if f.IsDir() {
						err = os.RemoveAll(filepath.Join(keyPath, f.Name()))
					} else {
						err = os.Remove(filepath.Join(keyPath, f.Name()))
					}
					if err != nil {
						return err
					}
				}
			}
			return nil
		} else {
			return storage.ErrKeyHasNoContent
		}
	}

	return ds.create(key, contents)
}

// create will make up a file with key as file path and contents as file contents.
func (ds *diskStorage) create(key string, contents []byte) error {
	if key == "" {
		return storage.ErrKeyIsEmpty
	}

	keyPath := filepath.Join(ds.baseDir, key)
	dir, _ := filepath.Split(keyPath)
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(dir, 0755); err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		// dir for key is already exist
	}

	// open file with synchronous I/O
	f, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0600)
	if err != nil {
		return err
	}
	n, err := f.Write(contents)
	if err == nil && n < len(contents) {
		err = io.ErrShortWrite
	}

	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// Delete delete file that specified by key
func (ds *diskStorage) Delete(key string) error {
	if key == "" {
		return storage.ErrKeyIsEmpty
	}

	if !ds.lockKey(key) {
		return storage.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)

	errs := make([]error, 0)
	if err := ds.delete(key); err != nil {
		errs = append(errs, err)
	}

	tmpKey := getTmpKey(key)
	if err := ds.delete(tmpKey); err != nil {
		errs = append(errs, err)
	}

	if len(errs) != 0 {
		return fmt.Errorf("%#+v", errs)
	}
	return nil
}

func (ds *diskStorage) delete(key string) error {
	if key == "" {
		return storage.ErrKeyIsEmpty
	}

	absKey := filepath.Join(ds.baseDir, key)
	info, err := os.Stat(absKey)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if info.Mode().IsRegular() {
		return os.Remove(absKey)
	}

	return nil
}

// Get get contents from the file that specified by key
func (ds *diskStorage) Get(key string) ([]byte, error) {
	if key == "" {
		return []byte{}, storage.ErrKeyIsEmpty
	}

	if !ds.lockKey(key) {
		return nil, storage.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)
	return ds.get(filepath.Join(ds.baseDir, key))
}

// get returns contents from the file of path
func (ds *diskStorage) get(path string) ([]byte, error) {
	if path == "" {
		return []byte{}, storage.ErrKeyIsEmpty
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []byte{}, storage.ErrStorageNotFound
		}
		return nil, fmt.Errorf("failed to get bytes from %s, %v", path, err)
	} else if info.Mode().IsRegular() {
		b, err := ioutil.ReadFile(path)
		if err != nil {
			return []byte{}, err
		}

		return b, nil
	} else if info.IsDir() {
		return []byte{}, storage.ErrKeyHasNoContent
	}

	return nil, fmt.Errorf("%s is exist, but not recognized, %v", path, info.Mode())
}

// ListKeys list all of keys for files
func (ds *diskStorage) ListKeys(key string) ([]string, error) {
	if key == "" {
		return []string{}, storage.ErrKeyIsEmpty
	}

	keys := make([]string, 0)
	absPath := filepath.Join(ds.baseDir, key)
	if info, err := os.Stat(absPath); err != nil {
		if os.IsNotExist(err) {
			return keys, nil
		}
		return keys, err
	} else if info.IsDir() {
		err := filepath.Walk(absPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.Mode().IsRegular() {
				if !isTmpFile(path) {
					keys = append(keys, strings.TrimPrefix(path, ds.baseDir))
				}
			}

			return nil
		})

		return keys, err
	} else if info.Mode().IsRegular() {
		keys = append(keys, key)
		return keys, nil
	}

	return keys, fmt.Errorf("failed to list keys because %s not recognized", key)
}

// List get all of contents for local files
func (ds *diskStorage) List(key string) ([][]byte, error) {
	if key == "" {
		return [][]byte{}, storage.ErrKeyIsEmpty
	}

	if !ds.lockKey(key) {
		return nil, storage.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)

	bb := make([][]byte, 0)
	absKey := filepath.Join(ds.baseDir, key)
	info, err := os.Stat(absKey)
	if err != nil {
		if os.IsNotExist(err) {
			return bb, storage.ErrStorageNotFound
		}
		klog.Errorf("filed to list bytes for (%s), %v", key, err)
		return nil, err
	} else if info.Mode().IsRegular() {
		b, err := ds.get(absKey)
		if err != nil {
			return nil, err
		}

		bb = append(bb, b)
		return bb, nil
	} else if info.Mode().IsDir() {
		err := filepath.Walk(absKey, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.Mode().IsRegular() && !isTmpFile(path) {
				b, err := ds.get(path)
				if err != nil {
					klog.Warningf("failed to get bytes for %s when listing bytes, %v", path, err)
					return nil
				}

				bb = append(bb, b)
			}

			return nil
		})

		if err != nil {
			return nil, err
		}

		return bb, nil
	}

	return nil, fmt.Errorf("%s is exist, but not recognized, %v", key, info.Mode())
}

// Update will update local file that specified by key with contents
func (ds *diskStorage) Update(key string, contents []byte) error {
	if key == "" {
		return storage.ErrKeyIsEmpty
	} else if len(contents) == 0 {
		return storage.ErrKeyHasNoContent
	}

	if !ds.lockKey(key) {
		return storage.ErrStorageAccessConflict
	}
	defer ds.unLockKey(key)

	// 1. create new file with tmpKey
	tmpKey := getTmpKey(key)
	err := ds.create(tmpKey, contents)
	if err != nil {
		return err
	}

	// 2. delete old file by key
	err = ds.delete(key)
	if err != nil {
		ds.delete(tmpKey)
		return err
	}

	// 3. rename tmpKey file to key file
	return os.Rename(filepath.Join(ds.baseDir, tmpKey), filepath.Join(ds.baseDir, key))
}

// Replace will delete all files under rootKey dir and create new files with contents.
func (ds *diskStorage) Replace(rootKey string, contents map[string][]byte) error {
	if rootKey == "" {
		return storage.ErrKeyIsEmpty
	} else if len(contents) == 0 {
		return storage.ErrKeyHasNoContent
	}

	for key := range contents {
		if !strings.Contains(key, rootKey) {
			return storage.ErrRootKeyInvalid
		}
	}

	if !ds.lockKey(rootKey) {
		return storage.ErrStorageAccessConflict
	}
	defer ds.unLockKey(rootKey)

	// 1. mv old dir into tmp_dir when rootKey dir already exists
	absPath := filepath.Join(ds.baseDir, rootKey)
	tmpRootKey := getTmpKey(rootKey)
	tmpPath := filepath.Join(ds.baseDir, tmpRootKey)
	dirExisted := false
	if info, err := os.Stat(absPath); err == nil {
		if info.IsDir() {
			err := os.Rename(absPath, tmpPath)
			if err != nil {
				return err
			}
			dirExisted = true
		}
	}

	// 2. create new file with contents
	// TODO: if error happens, we may need retry mechanism, or add some mechanism to do consistency check.
	for key, data := range contents {
		err := ds.create(key, data)
		if err != nil {
			klog.Errorf("failed to create %s in replace, %v", key, err)
			continue
		}
	}

	//  3. delete old tmp dir
	if dirExisted {
		return os.RemoveAll(tmpPath)
	}

	return nil
}

// DeleteCollection delete file or dir that specified by rootKey
func (ds *diskStorage) DeleteCollection(rootKey string) error {
	if rootKey == "" {
		return storage.ErrKeyIsEmpty
	}

	if !ds.lockKey(rootKey) {
		return storage.ErrStorageAccessConflict
	}
	defer ds.unLockKey(rootKey)

	absKey := filepath.Join(ds.baseDir, rootKey)
	info, err := os.Stat(absKey)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	} else if info.Mode().IsRegular() {
		return os.Remove(absKey)
	} else if info.IsDir() {
		return os.RemoveAll(absKey)
	}

	return fmt.Errorf("%s is exist, but not recognized, %v", rootKey, info.Mode())
}

// Recover recover storage error
func (ds *diskStorage) Recover(key string) error {
	if !ds.lockKey(key) {
		return nil
	}
	defer ds.unLockKey(key)

	dir := filepath.Join(ds.baseDir, key)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.Mode().IsRegular() {
			if isTmpFile(path) {
				tmpKey := strings.TrimPrefix(path, ds.baseDir)
				key := getKey(tmpKey)
				keyPath := filepath.Join(ds.baseDir, key)
				iErr := os.Rename(path, keyPath)
				if iErr != nil {
					klog.V(2).Infof("failed to recover bytes %s, %v", tmpKey, err)
					return nil
				}
				klog.V(2).Infof("bytes %s recovered successfully", key)
			}
		}

		return nil
	})

	return err
}

func (ds *diskStorage) lockKey(key string) bool {
	ds.Lock()
	defer ds.Unlock()
	if _, ok := ds.keyPendingStatus[key]; ok {
		return false
	}

	for pendingKey := range ds.keyPendingStatus {
		if len(key) > len(pendingKey) {
			if strings.Contains(key, pendingKey) {
				return false
			}
		} else {
			if strings.Contains(pendingKey, key) {
				return false
			}
		}
	}
	ds.keyPendingStatus[key] = struct{}{}
	return true
}

func (ds *diskStorage) unLockKey(key string) {
	ds.Lock()
	defer ds.Unlock()
	delete(ds.keyPendingStatus, key)
}

func getTmpKey(key string) string {
	dir, file := filepath.Split(key)
	return filepath.Join(dir, fmt.Sprintf("%s%s", tmpPrefix, file))
}

func isTmpFile(path string) bool {
	_, file := filepath.Split(path)
	if strings.HasPrefix(file, tmpPrefix) {
		return true
	}
	return false
}

func getKey(tmpKey string) string {
	dir, file := filepath.Split(tmpKey)
	return filepath.Join(dir, strings.TrimPrefix(file, tmpPrefix))
}
