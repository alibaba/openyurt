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

package revert

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"

	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurtctl/constants"
	"github.com/openyurtio/openyurt/pkg/yurtctl/lock"
	kubeutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/kubernetes"
	nodeutil "k8s.io/kubernetes/pkg/controller/util/node"
)

// RevertOptions has the information required by the revert operation
type RevertOptions struct {
	clientSet           *kubernetes.Clientset
	YurtctlServantImage string
	PodMainfestPath     string
	KubeadmConfPath     string
}

// NewConvertOptions creates a new RevertOptions
func NewRevertOptions() *RevertOptions {
	return &RevertOptions{}
}

// NewRevertCmd generates a new revert command
func NewRevertCmd() *cobra.Command {
	ro := NewRevertOptions()
	cmd := &cobra.Command{
		Use:   "revert",
		Short: "Reverts the yurt cluster back to a Kubernetes cluster",
		Run: func(cmd *cobra.Command, _ []string) {
			if err := ro.Complete(cmd.Flags()); err != nil {
				klog.Fatalf("fail to complete the revert option: %s", err)
			}
			if err := ro.RunRevert(); err != nil {
				klog.Fatalf("fail to revert yurt to kubernetes: %s", err)
			}
		},
	}

	cmd.AddCommand(NewRevertEdgeNodeCmd())

	cmd.Flags().String("yurtctl-servant-image",
		"openyurt/yurtctl-servant:latest",
		"The yurtctl-servant image.")
	cmd.Flags().String("pod-manifest-path",
		"/etc/kubernetes/manifests",
		"Path to the directory on edge node containing static pod files.")
	cmd.Flags().String("kubeadm-conf-path",
		"/etc/systemd/system/kubelet.service.d/10-kubeadm.conf",
		"The path to kubelet service conf that is used by kubelet component to join the cluster on the edge node.")

	return cmd
}

// Complete completes all the required options
func (ro *RevertOptions) Complete(flags *pflag.FlagSet) error {
	ycsi, err := flags.GetString("yurtctl-servant-image")
	if err != nil {
		return err
	}
	ro.YurtctlServantImage = ycsi

	pmp, err := flags.GetString("pod-manifest-path")
	if err != nil {
		return err
	}
	ro.PodMainfestPath = pmp

	kcp, err := flags.GetString("kubeadm-conf-path")
	if err != nil {
		return err
	}
	ro.KubeadmConfPath = kcp

	ro.clientSet, err = kubeutil.GenClientSet(flags)
	if err != nil {
		return err
	}
	return nil
}

// RunRevert reverts the target Yurt cluster back to a standard Kubernetes cluster
func (ro *RevertOptions) RunRevert() (err error) {
	if err = lock.AcquireLock(ro.clientSet); err != nil {
		return
	}
	defer func() {
		if releaseLockErr := lock.ReleaseLock(ro.clientSet); releaseLockErr != nil {
			klog.Error(releaseLockErr)
		}
	}()
	klog.V(4).Info("successfully acquire the lock")

	// 1. check the server version
	if err = kubeutil.ValidateServerVersion(ro.clientSet); err != nil {
		return
	}
	klog.V(4).Info("the server version is valid")

	// 1.1. check the state of worker nodes
	nodeLst, err := ro.clientSet.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return
	}

	for _, node := range nodeLst.Items {
		isEdgeNode, ok := node.Labels[projectinfo.GetEdgeWorkerLabelKey()]
		if ok && isEdgeNode == "true" {
			_, condition := nodeutil.GetNodeCondition(&node.Status, v1.NodeReady)
			if condition == nil || condition.Status != v1.ConditionTrue {
				klog.Errorf("Cannot do the revert, the status of worker node: %s is not 'Ready'.", node.Name)
				return
			}
		}
	}
	klog.V(4).Info("the status of worker nodes are satisfied")

	// 2. remove labels from nodes
	var edgeNodeNames []string
	for _, node := range nodeLst.Items {
		isEdgeNode, ok := node.Labels[projectinfo.GetEdgeWorkerLabelKey()]
		if ok && isEdgeNode == "true" {
			// cache edge nodes, we need to run servant job on each edge node later
			edgeNodeNames = append(edgeNodeNames, node.GetName())
		}
		if ok && isEdgeNode == "false" {
			// remove the label for both the cloud node
			delete(node.Labels, projectinfo.GetEdgeWorkerLabelKey())
			if _, err = ro.clientSet.CoreV1().Nodes().Update(context.Background(), &node, metav1.UpdateOptions{}); err != nil {
				return
			}
		}
	}
	klog.Info("label openyurt.io/is-edge-worker is removed")

	// 3. remove the yurt controller manager
	if err = ro.clientSet.AppsV1().Deployments("kube-system").
		Delete(context.Background(), "yurt-controller-manager", metav1.DeleteOptions{
			PropagationPolicy: &kubeutil.PropagationPolicy,
		}); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("fail to remove yurt controller manager: %s", err)
		return
	}
	klog.Info("yurt controller manager is removed")

	// 3.1 remove the serviceaccount for yurt-controller-manager
	if err = ro.clientSet.CoreV1().ServiceAccounts("kube-system").
		Delete(context.Background(), "yurt-controller-manager", metav1.DeleteOptions{
			PropagationPolicy: &kubeutil.PropagationPolicy,
		}); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("fail to remove serviceaccount for yurt controller manager: %s", err)
		return
	}
	klog.Info("serviceaccount for yurt controller manager is removed")

	// 3.2 remove the clusterrole for yurt-controller-manager
	if err = ro.clientSet.RbacV1().ClusterRoles().
		Delete(context.Background(), "yurt-controller-manager", metav1.DeleteOptions{
			PropagationPolicy: &kubeutil.PropagationPolicy,
		}); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("fail to remove clusterrole for yurt controller manager: %s", err)
		return
	}
	klog.Info("clusterrole for yurt controller manager is removed")

	// 3.3 remove the clusterrolebinding for yurt-controller-manager
	if err = ro.clientSet.RbacV1().ClusterRoleBindings().
		Delete(context.Background(), "yurt-controller-manager", metav1.DeleteOptions{
			PropagationPolicy: &kubeutil.PropagationPolicy,
		}); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("fail to remove clusterrolebinding for yurt controller manager: %s", err)
		return
	}
	klog.Info("clusterrolebinding for yurt controller manager is removed")

	// 4. remove the yurt-tunnel agent
	if err = removeYurtTunnelAgent(ro.clientSet); err != nil {
		klog.Errorf("fail to remove the yurt tunnel agent: %s", err)
		return
	}

	// 5. remove the yurt-tunnel server
	if err = removeYurtTunnelServer(ro.clientSet); err != nil {
		klog.Errorf("fail to remove the yurt tunnel server: %s", err)
		return
	}

	// 6. recreate the system:controller:node-controller clustrrolebinding
	ncClusterrolebinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "system:controller:node-controller",
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "system:controller:node-controller",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      "node-controller",
				Namespace: "kube-system",
			},
		},
	}
	if _, err = ro.clientSet.RbacV1().ClusterRoleBindings().Create(context.Background(), ncClusterrolebinding,
		metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		klog.Errorf("fail to create clusterrolebinding system:controller:node-controller: %v", err)
		return
	}
	klog.Info("clusterrolebinding system:controller:node-controller is created")

	// 7. remove yurt-hub and revert kubelet service
	if err = kubeutil.RunServantJobs(ro.clientSet,
		map[string]string{
			"action":                "revert",
			"yurtctl_servant_image": ro.YurtctlServantImage,
			"pod_manifest_path":     ro.PodMainfestPath,
			"kubeadm_conf_path":     ro.KubeadmConfPath,
		},
		edgeNodeNames, false); err != nil {
		klog.Errorf("fail to revert edge node: %s", err)
		return
	}
	klog.Info("yurt-hub is removed, kubelet service is reset")
	return
}

func removeYurtTunnelServer(client *kubernetes.Clientset) error {
	// 1. remove the DaemonSet
	if err := client.AppsV1().
		Deployments(constants.YurttunnelNamespace).
		Delete(context.Background(), constants.YurttunnelServerComponentName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the daemonset/%s: %s",
			constants.YurttunnelServerComponentName, err)
	}
	klog.V(4).Infof("deployment/%s is deleted", constants.YurttunnelServerComponentName)

	// 2.1 remove the Service
	if err := client.CoreV1().Services(constants.YurttunnelNamespace).
		Delete(context.Background(), constants.YurttunnelServerSvcName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the service/%s: %s",
			constants.YurttunnelServerSvcName, err)
	}
	klog.V(4).Infof("service/%s is deleted", constants.YurttunnelServerSvcName)

	// 2.2 remove the internal Service(type=ClusterIP)
	if err := client.CoreV1().Services(constants.YurttunnelNamespace).
		Delete(context.Background(), constants.YurttunnelServerInternalSvcName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the service/%s: %s",
			constants.YurttunnelServerInternalSvcName, err)
	}
	klog.V(4).Infof("service/%s is deleted", constants.YurttunnelServerInternalSvcName)

	// 3. remove the ClusterRoleBinding
	if err := client.RbacV1().ClusterRoleBindings().
		Delete(context.Background(), constants.YurttunnelServerComponentName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the clusterrolebinding/%s: %s",
			constants.YurttunnelServerComponentName, err)
	}
	klog.V(4).Infof("clusterrolebinding/%s is deleted", constants.YurttunnelServerComponentName)

	// 4. remove the SerivceAccount
	if err := client.CoreV1().ServiceAccounts(constants.YurttunnelNamespace).
		Delete(context.Background(), constants.YurttunnelServerComponentName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the serviceaccount/%s: %s",
			constants.YurttunnelServerComponentName, err)
	}
	klog.V(4).Infof("serviceaccount/%s is deleted", constants.YurttunnelServerComponentName)

	// 5. remove the ClusterRole
	if err := client.RbacV1().ClusterRoles().
		Delete(context.Background(), constants.YurttunnelServerComponentName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the clusterrole/%s: %s",
			constants.YurttunnelServerComponentName, err)
	}

	// 6. remove the ConfigMap
	if err := client.CoreV1().ConfigMaps(constants.YurttunnelNamespace).
		Delete(context.Background(), constants.YurttunnelServerCmName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the configmap/%s: %s",
			constants.YurttunnelServerCmName, err)
	}
	klog.V(4).Infof("clusterrole/%s is deleted", constants.YurttunnelServerComponentName)
	return nil
}

func removeYurtTunnelAgent(client *kubernetes.Clientset) error {
	// 1. remove the DaemonSet
	if err := client.AppsV1().
		DaemonSets(constants.YurttunnelNamespace).
		Delete(context.Background(), constants.YurttunnelAgentComponentName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the daemonset/%s: %s",
			constants.YurttunnelAgentComponentName, err)
	}
	klog.V(4).Infof("daemonset/%s is deleted", constants.YurttunnelAgentComponentName)

	// 2. remove the ClusterRoleBinding
	if err := client.RbacV1().ClusterRoleBindings().
		Delete(context.Background(), constants.YurttunnelAgentComponentName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the clusterrolebinding/%s: %s",
			constants.YurttunnelAgentComponentName, err)
	}
	klog.V(4).Infof("clusterrolebinding/%s is deleted", constants.YurttunnelAgentComponentName)

	// 3. remove the ClusterRole
	if err := client.RbacV1().ClusterRoles().
		Delete(context.Background(), constants.YurttunnelAgentComponentName,
			metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("fail to delete the clusterrole/%s: %s",
			constants.YurttunnelAgentComponentName, err)
	}
	klog.V(4).Infof("clusterrole/%s is deleted", constants.YurttunnelAgentComponentName)
	return nil
}
