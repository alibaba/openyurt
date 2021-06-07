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

package edgenode

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/klog"
)

// FileExists determines whether the file exists
func FileExists(filename string) (bool, error) {
	if _, err := os.Stat(filename); os.IsExist(err) {
		return true, err
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// GetContentFormFile returns all strings that match the regular expression regularExpression
func GetContentFormFile(filename string, regularExpression string) ([]string, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	ct := string(content)
	reg := regexp.MustCompile(regularExpression)
	res := reg.FindAllString(ct, -1)
	return res, nil
}

// GetSingleContentFromFile determines whether there is a unique string that matches the
// regular expression regularExpression and returns it
func GetSingleContentFromFile(filename string, regularExpression string) (string, error) {
	contents, err := GetContentFormFile(filename, regularExpression)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s, %v", filename, err)
	}
	if contents == nil {
		return "", fmt.Errorf("no matching string %s in file %s", regularExpression, filename)
	}
	if len(contents) > 1 {
		return "", fmt.Errorf("there are multiple matching string %s in file %s", regularExpression, filename)
	}
	return contents[0], nil
}

// DirExists determines whether the directory exists
func DirExists(dirname string) (bool, error) {
	s, err := os.Stat(dirname)
	if err != nil {
		return false, err
	}
	if !s.IsDir() {
		return false, fmt.Errorf("%s is not a dir", dirname)
	}
	return true, nil
}

// CopyFile copys sourceFile to destinationFile
func CopyFile(sourceFile string, destinationFile string) error {
	content, err := ioutil.ReadFile(sourceFile)
	if err != nil {
		return fmt.Errorf("failed to read source file %s: %v", sourceFile, err)
	}
	err = ioutil.WriteFile(destinationFile, content, 0666)
	if err != nil {
		return fmt.Errorf("failed to write destination file %s: %v", destinationFile, err)
	}
	return nil
}

// ReplaceRegularExpression matchs the regular expression and replace it with the corresponding string
func ReplaceRegularExpression(content string, replace map[string]string) string {
	for old, new := range replace {
		reg := regexp.MustCompile(old)
		content = reg.ReplaceAllString(content, new)
	}
	return content
}

// GetNodeName gets the node name based on environment variable, parameters --hostname-override
// in the configuration file or hostname
func GetNodeName(kubeadmConfPath string) (string, error) {
	//1. from env NODE_NAME
	nodename := os.Getenv("NODE_NAME")
	if nodename != "" {
		return nodename, nil
	}

	//2. find --hostname-override in 10-kubeadm.conf
	nodeName, err := GetSingleContentFromFile(kubeadmConfPath, KubeletHostname)
	if nodeName != "" {
		nodeName = strings.Split(nodeName, "=")[1]
		return nodeName, nil
	} else {
		klog.V(4).Info("get nodename err: ", err)
	}

	//3. find --hostname-override in EnvironmentFile
	environmentFiles, err := GetContentFormFile(kubeadmConfPath, KubeletEnvironmentFile)
	if err != nil {
		return "", err
	}
	for _, ef := range environmentFiles {
		ef = strings.Split(ef, "-")[1]
		nodeName, err = GetSingleContentFromFile(ef, KubeletHostname)
		if nodeName != "" {
			nodeName = strings.Split(nodeName, "=")[1]
			return nodeName, nil
		} else {
			klog.V(4).Info("get nodename err: ", err)
		}
	}

	//4. read nodeName from /etc/hostname
	content, err := ioutil.ReadFile(Hostname)
	if err != nil {
		return "", err
	}
	nodeName = strings.Trim(string(content), "\n")
	return nodeName, nil
}

// GenClientSet generates the clientset based on command option, environment variable,
// file in $HOME/.kube or the default kubeconfig file
func GenClientSet(flags *pflag.FlagSet) (*kubernetes.Clientset, error) {
	kubeconfigPath, err := PrepareKubeConfigPath(flags)
	if err != nil {
		return nil, err
	}

	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(restCfg)
}

// PrepareKubeConfigPath returns the path of cluster kubeconfig file
func PrepareKubeConfigPath(flags *pflag.FlagSet) (string, error) {
	kbCfgPath, err := flags.GetString("kubeconfig")
	if err != nil {
		return "", err
	}

	if kbCfgPath == "" {
		kbCfgPath = os.Getenv("KUBECONFIG")
	}

	if kbCfgPath == "" {
		if home := homedir.HomeDir(); home != "" {
			homeKbCfg := filepath.Join(home, ".kube", "config")
			if ok, _ := FileExists(homeKbCfg); ok {
				kbCfgPath = homeKbCfg
			}
		}
	}

	if kbCfgPath == "" {
		kbCfgPath = KubeCondfigPath
	}

	return kbCfgPath, nil
}

// Exec execs the command
func Exec(cmd *exec.Cmd) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}
