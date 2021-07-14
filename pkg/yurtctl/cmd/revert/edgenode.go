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

package revert

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	nodeutil "k8s.io/kubernetes/pkg/controller/util/node"

	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurtctl/constants"
	"github.com/openyurtio/openyurt/pkg/yurthub/certificate/hubself"
	enutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/edgenode"
	kubeutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/kubernetes"
	strutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/strings"
)

// RevertEdgeNodeOptions has the information required by sub command revert edgenode
type RevertEdgeNodeOptions struct {
	clientSet           *kubernetes.Clientset
	EdgeNodes           []string
	YurtctlServantImage string
	PodMainfestPath     string
	KubeadmConfPath     string
	openyurtDir         string
}

// NewRevertEdgeNodeOptions creates a new RevertEdgeNodeOptions
func NewRevertEdgeNodeOptions() *RevertEdgeNodeOptions {
	return &RevertEdgeNodeOptions{}
}

// NewRevertEdgeNodeCmd generates a new sub command revert edgenode
func NewRevertEdgeNodeCmd() *cobra.Command {
	r := NewRevertEdgeNodeOptions()
	cmd := &cobra.Command{
		Use:   "edgenode",
		Short: "reverts the yurt node to a kubernetes node",
		Run: func(cmd *cobra.Command, _ []string) {
			if err := r.Complete(cmd.Flags()); err != nil {
				klog.Fatalf("fail to complete the revert edgenode option: %s", err)
			}
			if err := r.RunRevertEdgeNode(); err != nil {
				klog.Fatalf("fail to revert the yurt node to a kubernetes node: %s", err)
			}
		},
	}

	cmd.Flags().StringP("edge-nodes", "e", "",
		"The list of edge nodes wanted to be revert.(e.g. -e edgenode1,edgenode2)")
	cmd.Flags().String("yurtctl-servant-image", "openyurt/yurtctl-servant:latest",
		"The yurtctl-servant image.")
	cmd.Flags().String("pod-manifest-path", "",
		"Path to the directory on edge node containing static pod files.")
	cmd.Flags().String("kubeadm-conf-path", "",
		"The path to kubelet service conf that is used by kubelet component to join the cluster on the edge node.")

	return cmd
}

// Complete completes all the required options
func (r *RevertEdgeNodeOptions) Complete(flags *pflag.FlagSet) (err error) {
	enStr, err := flags.GetString("edge-nodes")
	if err != nil {
		return err
	}
	if enStr != "" {
		r.EdgeNodes = strings.Split(enStr, ",")
	}

	ycsi, err := flags.GetString("yurtctl-servant-image")
	if err != nil {
		return err
	}
	r.YurtctlServantImage = ycsi

	podMainfestPath, err := flags.GetString("pod-manifest-path")
	if err != nil {
		return err
	}
	if podMainfestPath == "" {
		podMainfestPath = os.Getenv("STATIC_POD_PATH")
	}
	if podMainfestPath == "" {
		podMainfestPath = enutil.StaticPodPath
	}
	r.PodMainfestPath = podMainfestPath

	kubeadmConfPath, err := flags.GetString("kubeadm-conf-path")
	if err != nil {
		return err
	}
	if kubeadmConfPath == "" {
		kubeadmConfPath = os.Getenv("KUBELET_SVC")
	}
	if kubeadmConfPath == "" {
		kubeadmConfPath = enutil.KubeletSvcPath
	}
	r.KubeadmConfPath = kubeadmConfPath

	r.clientSet, err = enutil.GenClientSet(flags)
	if err != nil {
		return err
	}

	openyurtDir := os.Getenv("OPENYURT_DIR")
	if openyurtDir == "" {
		openyurtDir = enutil.OpenyurtDir
	}
	r.openyurtDir = openyurtDir

	return
}

// RunRevertEdgeNode reverts the target Yurt node back to a standard Kubernetes node
func (r *RevertEdgeNodeOptions) RunRevertEdgeNode() (err error) {
	// 1. check the server version
	if err = kubeutil.ValidateServerVersion(r.clientSet); err != nil {
		return
	}
	klog.V(4).Info("the server version is valid")

	nodeName, err := enutil.GetNodeName(r.KubeadmConfPath)
	if err != nil {
		nodeName = ""
	}
	if len(r.EdgeNodes) > 1 || (len(r.EdgeNodes) == 1 && r.EdgeNodes[0] != nodeName) {
		// 2. remote edgenode revert
		nodeLst, err := r.clientSet.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return err
		}

		// 2.1. check the EdgeNodes and its label
		var edgeNodeNames []string
		for _, node := range nodeLst.Items {
			isEdgeNode, ok := node.Labels[projectinfo.GetEdgeWorkerLabelKey()]
			if ok && isEdgeNode == "true" {
				edgeNodeNames = append(edgeNodeNames, node.GetName())
			}
		}
		for _, edgeNode := range r.EdgeNodes {
			if !strutil.IsInStringLst(edgeNodeNames, edgeNode) {
				klog.Errorf("Cannot do the revert, the worker node: %s is not a Yurt edge node.", edgeNode)
				return err
			}
		}

		// 2.2. check the state of EdgeNodes
		for _, node := range nodeLst.Items {
			if strutil.IsInStringLst(r.EdgeNodes, node.GetName()) {
				_, condition := nodeutil.GetNodeCondition(&node.Status, v1.NodeReady)
				if condition == nil || condition.Status != v1.ConditionTrue {
					klog.Errorf("Cannot do the revert, the status of worker node: %s is not 'Ready'.", node.Name)
					return err
				}
			}
		}

		// 2.3. remove yurt-hub and revert kubelet service
		if err = kubeutil.RunServantJobs(r.clientSet,
			map[string]string{
				"action":                "revert",
				"yurtctl_servant_image": r.YurtctlServantImage,
				"pod_manifest_path":     r.PodMainfestPath,
				"kubeadm_conf_path":     r.KubeadmConfPath,
			},
			r.EdgeNodes, false); err != nil {
			klog.Errorf("fail to revert edge node: %s", err)
			return err
		}
	} else if (len(r.EdgeNodes) == 0 && nodeName != "") || (len(r.EdgeNodes) == 1 && r.EdgeNodes[0] == nodeName) {
		// 3. local edgenode revert
		// 3.1. check if critical files exist
		yurtKubeletConf := r.getYurthubKubeletConf()
		if ok, err := enutil.FileExists(yurtKubeletConf); !ok {
			return err
		}
		kubeletSvcBk := r.getKubeletSvcBackup()
		if _, err := enutil.FileExists(kubeletSvcBk); err != nil {
			klog.Errorf("fail to get file %s, should revise the %s directly", kubeletSvcBk, r.KubeadmConfPath)
			return err
		}
		yurthubYaml := r.getYurthubYaml()
		if ok, err := enutil.FileExists(yurthubYaml); !ok {
			return err
		}

		// 3.2. check the state of EdgeNodes
		node, err := r.clientSet.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		_, condition := nodeutil.GetNodeCondition(&node.Status, v1.NodeReady)
		if condition == nil || condition.Status != v1.ConditionTrue {
			klog.Errorf("Cannot do the revert, the status of worker node: %s is not 'Ready'.", node.Name)
			return err
		}

		// 3.3. check the label of EdgeNodes
		isEdgeNode, ok := node.Labels[projectinfo.GetEdgeWorkerLabelKey()]
		if !ok || isEdgeNode == "false" {
			return fmt.Errorf("Cannot do the revert, the worker node: %s is not a Yurt edge node.", node.Name)
		}

		// 3.4. remove yurt-hub and revert kubelet service
		if err := r.RevertKubelet(); err != nil {
			return fmt.Errorf("fail to revert kubelet: %v", err)
		}
		if err := r.RemoveYurthub(); err != nil {
			return err
		}

		// 3.5. remove label of EdgeNodes
		node, err = r.clientSet.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		_, foundAutonomy := node.Annotations[constants.AnnotationAutonomy]
		if foundAutonomy {
			delete(node.Annotations, constants.AnnotationAutonomy)
		}
		delete(node.Labels, projectinfo.GetEdgeWorkerLabelKey())
		if _, err = r.clientSet.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
			return err
		}
		klog.Info("label openyurt.io/is-edge-worker is removed")

	} else {
		return fmt.Errorf("fail to revert edge node, flag --edge-nodes %s err", r.EdgeNodes)
	}

	return nil
}

// RevertKubelet resets the kubelet service
func (r *RevertEdgeNodeOptions) RevertKubelet() error {
	// 1. remove openyurt's kubelet.conf if exist
	yurtKubeletConf := r.getYurthubKubeletConf()
	if err := os.Remove(yurtKubeletConf); err != nil {
		return err
	}
	kubeletSvcBk := r.getKubeletSvcBackup()
	klog.Infof("found backup file %s, will use it to revert the node", kubeletSvcBk)
	err := os.Rename(kubeletSvcBk, r.KubeadmConfPath)
	if err != nil {
		return err
	}

	// 2. reset the kubelet.service
	klog.Info(enutil.DaemonReload)
	cmd := exec.Command("bash", "-c", enutil.DaemonReload)
	if err := enutil.Exec(cmd); err != nil {
		return err
	}

	klog.Info(enutil.RestartKubeletSvc)
	cmd = exec.Command("bash", "-c", enutil.RestartKubeletSvc)
	if err := enutil.Exec(cmd); err != nil {
		return err
	}
	klog.Infof("kubelet has been reset back to default")
	return nil
}

// RemoveYurthub deletes the yurt-hub pod
func (r *RevertEdgeNodeOptions) RemoveYurthub() error {
	// 1. remove the yurt-hub.yaml to delete the yurt-hub
	yurthubYaml := r.getYurthubYaml()
	err := os.Remove(yurthubYaml)
	if err != nil {
		return err
	}

	// 2. remove yurt-hub config directory and certificates in it
	yurthubConf := r.getYurthubConf()
	err = os.RemoveAll(yurthubConf)
	if err != nil {
		return err
	}
	klog.Infof("yurt-hub has been removed")
	return nil
}

func (r *RevertEdgeNodeOptions) getYurthubKubeletConf() string {
	return filepath.Join(r.openyurtDir, enutil.KubeletConfName)
}

func (r *RevertEdgeNodeOptions) getKubeletSvcBackup() string {
	return fmt.Sprintf(enutil.KubeletSvcBackup, r.KubeadmConfPath)
}

func (r *RevertEdgeNodeOptions) getYurthubYaml() string {
	return filepath.Join(r.PodMainfestPath, enutil.YurthubYamlName)
}

func (r *RevertEdgeNodeOptions) getYurthubConf() string {
	return filepath.Join(hubself.HubRootDir, hubself.HubName)
}
