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

package convert

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"k8s.io/klog"
	clusterinfophase "k8s.io/kubernetes/cmd/kubeadm/app/phases/bootstraptoken/clusterinfo"
	nodeutil "k8s.io/kubernetes/pkg/controller/util/node"

	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurtctl/constants"
	"github.com/openyurtio/openyurt/pkg/yurtctl/lock"
	kubeutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/kubernetes"
	strutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/strings"
)

// Provider signifies the provider type
type Provider string

const (
	// ProviderMinikube is used if the target kubernetes is run on minikube
	ProviderMinikube Provider = "minikube"
	// ProviderACK is used if the target kubernetes is run on ack
	ProviderACK     Provider = "ack"
	ProviderKubeadm Provider = "kubeadm"
	// ProviderKind is used if the target kubernetes is run on kind
	ProviderKind Provider = "kind"
)

const (
	// defaultYurthubHealthCheckTimeout defines the default timeout for yurthub health check phase
	defaultYurthubHealthCheckTimeout = 2 * time.Minute
)

// ConvertOptions has the information that required by convert operation
type ConvertOptions struct {
	clientSet                  *kubernetes.Clientset
	CloudNodes                 []string
	Provider                   Provider
	YurhubImage                string
	YurthubHealthCheckTimeout  time.Duration
	YurtControllerManagerImage string
	YurctlServantImage         string
	YurttunnelServerImage      string
	YurttunnelAgentImage       string
	PodMainfestPath            string
	KubeadmConfPath            string
	DeployTunnel               bool
	kubeConfigPath             string
}

// NewConvertOptions creates a new ConvertOptions
func NewConvertOptions() *ConvertOptions {
	return &ConvertOptions{
		CloudNodes: []string{},
	}
}

// NewConvertCmd generates a new convert command
func NewConvertCmd() *cobra.Command {
	co := NewConvertOptions()
	cmd := &cobra.Command{
		Use:   "convert -c CLOUDNODES",
		Short: "Converts the kubernetes cluster to a yurt cluster",
		Run: func(cmd *cobra.Command, _ []string) {
			if err := co.Complete(cmd.Flags()); err != nil {
				klog.Fatalf("fail to complete the convert option: %s", err)
			}
			if err := co.Validate(); err != nil {
				klog.Fatalf("convert option is invalid: %s", err)
			}
			if err := co.RunConvert(); err != nil {
				klog.Fatalf("fail to convert kubernetes to yurt: %s", err)
			}
		},
	}

	cmd.AddCommand(NewConvertEdgeNodeCmd())

	cmd.Flags().StringP("cloud-nodes", "c", "",
		"The list of cloud nodes.(e.g. -c cloudnode1,cloudnode2)")
	cmd.Flags().StringP("provider", "p", "minikube",
		"The provider of the original Kubernetes cluster.")
	cmd.Flags().String("yurthub-image",
		"openyurt/yurthub:latest",
		"The yurthub image.")
	cmd.Flags().Duration("yurthub-healthcheck-timeout", defaultYurthubHealthCheckTimeout,
		"The timeout for yurthub health check.")
	cmd.Flags().String("yurt-controller-manager-image",
		"openyurt/yurt-controller-manager:latest",
		"The yurt-controller-manager image.")
	cmd.Flags().String("yurtctl-servant-image",
		"openyurt/yurtctl-servant:latest",
		"The yurtctl-servant image.")
	cmd.Flags().String("kubeadm-conf-path",
		"/etc/systemd/system/kubelet.service.d/10-kubeadm.conf",
		"The path to kubelet service conf that is used by kubelet component to join the cluster on the edge node.")
	cmd.Flags().String("yurt-tunnel-server-image",
		"openyurt/yurt-tunnel-server:latest",
		"The yurt-tunnel-server image.")
	cmd.Flags().String("yurt-tunnel-agent-image",
		"openyurt/yurt-tunnel-agent:latest",
		"The yurt-tunnel-agent image.")
	cmd.Flags().BoolP("deploy-yurttunnel", "t", false,
		"if set, yurttunnel will be deployed.")
	cmd.Flags().String("pod-manifest-path",
		"/etc/kubernetes/manifests",
		"Path to the directory on edge node containing static pod files.")

	return cmd
}

// Complete completes all the required options
func (co *ConvertOptions) Complete(flags *pflag.FlagSet) error {
	cnStr, err := flags.GetString("cloud-nodes")
	if err != nil {
		return err
	}
	if cnStr != "" {
		co.CloudNodes = strings.Split(cnStr, ",")
	} else {
		err := fmt.Errorf("The '--cloud nodes' parameter cannot be empty.Please specify the cloud node first, and then execute the yurtctl convert command")
		return err
	}

	dt, err := flags.GetBool("deploy-yurttunnel")
	if err != nil {
		return err
	}
	co.DeployTunnel = dt

	pStr, err := flags.GetString("provider")
	if err != nil {
		return err
	}
	co.Provider = Provider(pStr)

	yhi, err := flags.GetString("yurthub-image")
	if err != nil {
		return err
	}
	co.YurhubImage = yhi

	yurthubHealthCheckTimeout, err := flags.GetDuration("yurthub-healthcheck-timeout")
	if err != nil {
		return err
	}
	co.YurthubHealthCheckTimeout = yurthubHealthCheckTimeout

	ycmi, err := flags.GetString("yurt-controller-manager-image")
	if err != nil {
		return err
	}
	co.YurtControllerManagerImage = ycmi

	ycsi, err := flags.GetString("yurtctl-servant-image")
	if err != nil {
		return err
	}
	co.YurctlServantImage = ycsi

	ytsi, err := flags.GetString("yurt-tunnel-server-image")
	if err != nil {
		return err
	}
	co.YurttunnelServerImage = ytsi

	ytai, err := flags.GetString("yurt-tunnel-agent-image")
	if err != nil {
		return err
	}
	co.YurttunnelAgentImage = ytai

	pmp, err := flags.GetString("pod-manifest-path")
	if err != nil {
		return err
	}
	co.PodMainfestPath = pmp

	kcp, err := flags.GetString("kubeadm-conf-path")
	if err != nil {
		return err
	}
	co.KubeadmConfPath = kcp

	// parse kubeconfig and generate the clientset
	co.clientSet, err = kubeutil.GenClientSet(flags)
	if err != nil {
		return err
	}

	// prepare path of cluster kubeconfig file
	co.kubeConfigPath, err = kubeutil.PrepareKubeConfigPath(flags)
	if err != nil {
		return err
	}
	return nil
}

// Validate makes sure provided values for ConvertOptions are valid
func (co *ConvertOptions) Validate() error {
	if co.Provider != ProviderMinikube && co.Provider != ProviderACK &&
		co.Provider != ProviderKubeadm && co.Provider != ProviderKind {
		return fmt.Errorf("unknown provider: %s, valid providers are: minikube, ack, kubeadm, kind",
			co.Provider)
	}

	return nil
}

// RunConvert performs the conversion
func (co *ConvertOptions) RunConvert() (err error) {
	if err = lock.AcquireLock(co.clientSet); err != nil {
		return
	}
	defer func() {
		if releaseLockErr := lock.ReleaseLock(co.clientSet); releaseLockErr != nil {
			klog.Error(releaseLockErr)
		}
	}()
	klog.V(4).Info("successfully acquire the lock")

	// 1. check the server version
	if err = kubeutil.ValidateServerVersion(co.clientSet); err != nil {
		return
	}
	klog.V(4).Info("the server version is valid")

	// 1.1. check the state of worker nodes
	nodeLst, err := co.clientSet.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, node := range nodeLst.Items {
		if !strutil.IsInStringLst(co.CloudNodes, node.GetName()) {
			_, condition := nodeutil.GetNodeCondition(&node.Status, v1.NodeReady)
			if condition == nil || condition.Status != v1.ConditionTrue {
				klog.Errorf("Cannot do the convert, the status of worker node: %s is not 'Ready'.", node.Name)
				return
			}
		}
	}
	klog.V(4).Info("the status of worker nodes are satisfied")

	// 2. label nodes as cloud node or edge node
	var edgeNodeNames []string
	for _, node := range nodeLst.Items {
		if strutil.IsInStringLst(co.CloudNodes, node.GetName()) {
			// label node as cloud node
			klog.Infof("mark %s as the cloud-node", node.GetName())
			if _, err = kubeutil.LabelNode(co.clientSet,
				&node, projectinfo.GetEdgeWorkerLabelKey(), "false"); err != nil {
				return
			}
			continue
		}
		edgeNodeNames = append(edgeNodeNames, node.GetName())
	}

	// 3. deploy yurt controller manager
	// create a service account for yurt-controller-manager
	err = kubeutil.CreateServiceAccountFromYaml(co.clientSet,
		"kube-system", constants.YurtControllerManagerServiceAccount)
	if err != nil {
		return
	}
	// create the clusterrole
	err = kubeutil.CreateClusterRoleFromYaml(co.clientSet,
		constants.YurtControllerManagerClusterRole)
	if err != nil {
		return err
	}
	// bind the clusterrole
	err = kubeutil.CreateClusterRoleBindingFromYaml(co.clientSet,
		constants.YurtControllerManagerClusterRoleBinding)
	if err != nil {
		return
	}
	// create the yurt-controller-manager deployment
	err = kubeutil.CreateDeployFromYaml(co.clientSet,
		"kube-system",
		constants.YurtControllerManagerDeployment,
		map[string]string{
			"image":         co.YurtControllerManagerImage,
			"edgeNodeLabel": projectinfo.GetEdgeWorkerLabelKey()})
	if err != nil {
		return
	}

	// 4. delete the system:controller:node-controller clusterrolebinding to disable node-controller
	if err = co.clientSet.RbacV1().ClusterRoleBindings().Delete(context.Background(), "system:controller:node-controller", metav1.DeleteOptions{
		PropagationPolicy: &kubeutil.PropagationPolicy,
	}); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("fail to delete clusterrolebinding system:controller:node-controller: %v", err)
		return
	}

	// 5. deploy the yurttunnel if required
	if co.DeployTunnel {
		if err = deployYurttunnelServer(co.clientSet,
			co.CloudNodes,
			co.YurttunnelServerImage); err != nil {
			err = fmt.Errorf("fail to deploy the yurt-tunnel-server: %s", err)
			return
		}
		klog.Info("yurt-tunnel-server is deployed")
		// we will deploy yurt-tunnel-agent on every edge node
		if err = deployYurttunnelAgent(co.clientSet,
			edgeNodeNames,
			co.YurttunnelAgentImage); err != nil {
			err = fmt.Errorf("fail to deploy the yurt-tunnel-agent: %s", err)
			return
		}
		klog.Info("yurt-tunnel-agent is deployed")
	}

	// 6. prepare kube-public/cluster-info configmap before convert
	err = prepareClusterInfoConfigMap(co.clientSet, co.kubeConfigPath)
	if err != nil {
		klog.Errorf("fail to prepre cluster-info configmap, %v", err)
		return
	}

	// 7. deploy yurt-hub and reset the kubelet service
	klog.Infof("deploying the yurt-hub and resetting the kubelet service...")
	joinToken, err := kubeutil.GetOrCreateJoinTokenString(co.clientSet)
	if err != nil {
		return err
	}

	ctx := map[string]string{
		"provider":              string(co.Provider),
		"action":                "convert",
		"yurtctl_servant_image": co.YurctlServantImage,
		"yurthub_image":         co.YurhubImage,
		"joinToken":             joinToken,
		"pod_manifest_path":     co.PodMainfestPath,
		"kubeadm_conf_path":     co.KubeadmConfPath,
	}

	if co.YurthubHealthCheckTimeout != defaultYurthubHealthCheckTimeout {
		ctx["yurthub_healthcheck_timeout"] = co.YurthubHealthCheckTimeout.String()
	}

	if err = kubeutil.RunServantJobs(co.clientSet, ctx, edgeNodeNames, true); err != nil {
		klog.Errorf("fail to run ServantJobs: %s", err)
		return
	}
	klog.Info("the yurt-hub is deployed")

	return
}

func deployYurttunnelServer(
	client *kubernetes.Clientset,
	cloudNodes []string,
	yurttunnelServerImage string) error {
	// 1. create the ClusterRole
	if err := kubeutil.CreateClusterRoleFromYaml(client,
		constants.YurttunnelServerClusterRole); err != nil {
		return err
	}

	// 2. create the ServiceAccount
	if err := kubeutil.CreateServiceAccountFromYaml(client, "kube-system",
		constants.YurttunnelServerServiceAccount); err != nil {
		return err
	}

	// 3. create the ClusterRoleBinding
	if err := kubeutil.CreateClusterRoleBindingFromYaml(client,
		constants.YurttunnelServerClusterRolebinding); err != nil {
		return err
	}

	// 4. create the Service
	if err := kubeutil.CreateServiceFromYaml(client,
		constants.YurttunnelServerService); err != nil {
		return err
	}

	// 5. create the internal Service(type=ClusterIP)
	if err := kubeutil.CreateServiceFromYaml(client,
		constants.YurttunnelServerInternalService); err != nil {
		return err
	}

	// 6. create the Configmap
	if err := kubeutil.CreateConfigMapFromYaml(client,
		"kube-system",
		constants.YurttunnelServerConfigMap); err != nil {
		return err
	}

	// 7. create the Deployment
	if err := kubeutil.CreateDeployFromYaml(client,
		"kube-system",
		constants.YurttunnelServerDeployment,
		map[string]string{
			"image":           yurttunnelServerImage,
			"edgeWorkerLabel": projectinfo.GetEdgeWorkerLabelKey()}); err != nil {
		return err
	}

	return nil
}

func deployYurttunnelAgent(
	client *kubernetes.Clientset,
	tunnelAgentNodes []string,
	yurttunnelAgentImage string) error {
	// 1. Deploy the yurt-tunnel-agent DaemonSet
	if err := kubeutil.CreateDaemonSetFromYaml(client,
		constants.YurttunnelAgentDaemonSet,
		map[string]string{
			"image":           yurttunnelAgentImage,
			"edgeWorkerLabel": projectinfo.GetEdgeWorkerLabelKey()}); err != nil {
		return err
	}
	return nil
}

// prepareClusterInfoConfigMap will create cluster-info configmap in kube-public namespace if it does not exist
func prepareClusterInfoConfigMap(client *kubernetes.Clientset, file string) error {
	info, err := client.CoreV1().ConfigMaps(metav1.NamespacePublic).Get(context.Background(), bootstrapapi.ConfigMapClusterInfo, metav1.GetOptions{})
	if err != nil && apierrors.IsNotFound(err) {
		// Create the cluster-info ConfigMap with the associated RBAC rules
		if err := clusterinfophase.CreateBootstrapConfigMapIfNotExists(client, file); err != nil {
			return fmt.Errorf("error creating bootstrap ConfigMap, %v", err)
		}
		if err := clusterinfophase.CreateClusterInfoRBACRules(client); err != nil {
			return fmt.Errorf("error creating clusterinfo RBAC rules, %v", err)
		}
	} else if err != nil || info == nil {
		klog.Errorf("fail to get configmap, %v", err)
		return fmt.Errorf("fail to get configmap, %v", err)
	} else {
		klog.Infof("%s/%s configmap already exists, skip to prepare it", info.Namespace, info.Name)
	}

	return nil
}
