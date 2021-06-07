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

package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/pflag"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	bootstraputil "k8s.io/cluster-bootstrap/token/util"
	"k8s.io/klog"
	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmcontants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	tokenphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/bootstraptoken/node"

	"github.com/openyurtio/openyurt/pkg/yurtctl/constants"
	strutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/strings"
	tmplutil "github.com/openyurtio/openyurt/pkg/yurtctl/util/templates"
)

const (
	// ConvertJobNameBase is the prefix of the convert ServantJob name
	ConvertJobNameBase = "yurtctl-servant-convert"
	// RevertJobNameBase is the prefix of the revert ServantJob name
	RevertJobNameBase = "yurtctl-servant-revert"
)

var (
	// PropagationPolicy defines the propagation policy used when deleting a resource
	PropagationPolicy = metav1.DeletePropagationBackground
	// WaitServantJobTimeout specifies the timeout value of waiting for the ServantJob to be succeeded
	WaitServantJobTimeout = time.Minute * 2
	// CheckServantJobPeriod defines the time interval between two successive ServantJob statu's inspection
	CheckServantJobPeriod = time.Second * 10
	// ValidServerVersions contains all compatable server version
	// yurtctl only support Kubernetes 1.12+ - 1.16+ for now
	ValidServerVersions = []string{
		"1.12", "1.12+",
		"1.13", "1.13+",
		"1.14", "1.14+",
		"1.16", "1.16+",
		"1.18", "1.18+",
		"1.20", "1.20+"}
)

// CreateServiceAccountFromYaml creates the ServiceAccount from the yaml template.
func CreateServiceAccountFromYaml(cliSet *kubernetes.Clientset, ns, saTmpl string) error {
	obj, err := YamlToObject([]byte(saTmpl))
	if err != nil {
		return err
	}
	sa, ok := obj.(*corev1.ServiceAccount)
	if !ok {
		return fmt.Errorf("fail to assert serviceaccount: %v", err)
	}
	_, err = cliSet.CoreV1().ServiceAccounts(ns).Create(context.Background(), sa, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("fail to create the serviceaccount/%s: %v", sa.Name, err)
	}
	klog.V(4).Infof("serviceaccount/%s is created", sa.Name)
	return nil
}

// CreateClusterRoleFromYaml creates the ClusterRole from the yaml template.
func CreateClusterRoleFromYaml(cliSet *kubernetes.Clientset, crTmpl string) error {
	obj, err := YamlToObject([]byte(crTmpl))
	if err != nil {
		return err
	}
	cr, ok := obj.(*rbacv1.ClusterRole)
	if !ok {
		return fmt.Errorf("fail to assert clusterrole: %v", err)
	}
	_, err = cliSet.RbacV1().ClusterRoles().Create(context.Background(), cr, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("fail to create the clusterrole/%s: %v", cr.Name, err)
	}
	klog.V(4).Infof("clusterrole/%s is created", cr.Name)
	return nil
}

// CreateClusterRoleBindingFromYaml creates the ClusterRoleBinding from the yaml template.
func CreateClusterRoleBindingFromYaml(cliSet *kubernetes.Clientset, crbTmpl string) error {
	obj, err := YamlToObject([]byte(crbTmpl))
	if err != nil {
		return err
	}
	crb, ok := obj.(*rbacv1.ClusterRoleBinding)
	if !ok {
		return fmt.Errorf("fail to assert clusterrolebinding: %v", err)
	}
	_, err = cliSet.RbacV1().ClusterRoleBindings().Create(context.Background(), crb, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("fail to create the clusterrolebinding/%s: %v", crb.Name, err)
	}
	klog.V(4).Infof("clusterrolebinding/%s is created", crb.Name)
	return nil
}

// CreateConfigMapFromYaml creates the ConfigMap from the yaml template.
func CreateConfigMapFromYaml(cliSet *kubernetes.Clientset, ns, cmTmpl string) error {
	obj, err := YamlToObject([]byte(cmTmpl))
	if err != nil {
		return err
	}
	cm, ok := obj.(*v1.ConfigMap)
	if !ok {
		return fmt.Errorf("fail to assert configmap: %v", err)
	}
	_, err = cliSet.CoreV1().ConfigMaps(ns).Create(context.Background(), cm, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("fail to create the configmap/%s: %v", cm.Name, err)
	}
	klog.V(4).Infof("configmap/%s is created", cm.Name)
	return nil
}

// CreateDeployFromYaml creates the Deployment from the yaml template.
func CreateDeployFromYaml(cliSet *kubernetes.Clientset, ns, dplyTmpl string, ctx interface{}) error {
	ycmdp, err := tmplutil.SubsituteTemplate(dplyTmpl, ctx)
	if err != nil {
		return err
	}
	dpObj, err := YamlToObject([]byte(ycmdp))
	if err != nil {
		return err
	}
	dply, ok := dpObj.(*appsv1.Deployment)
	if !ok {
		return errors.New("fail to assert Deployment")
	}
	if _, err = cliSet.AppsV1().Deployments(ns).Create(context.Background(), dply, metav1.CreateOptions{}); err != nil {
		return err
	}
	klog.V(4).Infof("the deployment/%s is deployed", dply.Name)
	return nil
}

// CreateDaemonSetFromYaml creates the DaemonSet from the yaml template.
func CreateDaemonSetFromYaml(cliSet *kubernetes.Clientset, dsTmpl string, ctx interface{}) error {
	ytadstmp, err := tmplutil.SubsituteTemplate(dsTmpl, ctx)
	if err != nil {
		return err
	}
	obj, err := YamlToObject([]byte(ytadstmp))
	if err != nil {
		return err
	}
	ds, ok := obj.(*appsv1.DaemonSet)
	if !ok {
		return fmt.Errorf("fail to assert daemonset: %v", err)
	}
	_, err = cliSet.AppsV1().DaemonSets("kube-system").Create(context.Background(), ds, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("fail to create the daemonset/%s: %v", ds.Name, err)
	}
	klog.V(4).Infof("daemonset/%s is created", ds.Name)
	return nil
}

// CreateServiceFromYaml creates the Service from the yaml template.
func CreateServiceFromYaml(cliSet *kubernetes.Clientset, svcTmpl string) error {
	obj, err := YamlToObject([]byte(svcTmpl))
	if err != nil {
		return err
	}
	svc, ok := obj.(*corev1.Service)
	if !ok {
		return fmt.Errorf("fail to assert service: %v", err)
	}
	_, err = cliSet.CoreV1().Services("kube-system").Create(context.Background(), svc, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("fail to create the service/%s: %s", svc.Name, err)
	}
	klog.V(4).Infof("service/%s is created", svc.Name)
	return nil
}

// YamlToObject deserializes object in yaml format to a runtime.Object
func YamlToObject(yamlContent []byte) (runtime.Object, error) {
	decode := serializer.NewCodecFactory(scheme.Scheme).UniversalDeserializer().Decode
	obj, _, err := decode(yamlContent, nil, nil)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// LabelNode add a new label (<key>=<val>) to the given node
func LabelNode(cliSet *kubernetes.Clientset, node *v1.Node, key, val string) (*v1.Node, error) {
	node.Labels[key] = val
	newNode, err := cliSet.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return newNode, nil
}

// AnnotateNode add a new annotation (<key>=<val>) to the given node
func AnnotateNode(cliSet *kubernetes.Clientset, node *v1.Node, key, val string) (*v1.Node, error) {
	node.Annotations[key] = val
	newNode, err := cliSet.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return newNode, nil
}

// RunJobAndCleanup runs the job, wait for it to be complete, and delete it
func RunJobAndCleanup(cliSet *kubernetes.Clientset, job *batchv1.Job, timeout, period time.Duration) error {
	job, err := cliSet.BatchV1().Jobs(job.GetNamespace()).Create(context.Background(), job, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	waitJobTimeout := time.After(timeout)
	for {
		select {
		case <-waitJobTimeout:
			return errors.New("wait for job to be complete timeout")
		case <-time.After(period):
			job, err := cliSet.BatchV1().Jobs(job.GetNamespace()).
				Get(context.Background(), job.GetName(), metav1.GetOptions{})
			if err != nil {
				klog.Errorf("fail to get job(%s) when waiting for it to be succeeded: %s",
					job.GetName(), err)
				return err
			}
			if job.Status.Succeeded == *job.Spec.Completions {
				if err := cliSet.BatchV1().Jobs(job.GetNamespace()).
					Delete(context.Background(), job.GetName(), metav1.DeleteOptions{
						PropagationPolicy: &PropagationPolicy,
					}); err != nil {
					klog.Errorf("fail to delete succeeded servant job(%s): %s",
						job.GetName(), err)
					return err
				}
				return nil
			}
			continue
		}
	}
}

// RunServantJobs launchs servant jobs on specified edge nodes
func RunServantJobs(cliSet *kubernetes.Clientset, tmplCtx map[string]string, edgeNodeNames []string, convert bool) error {
	var wg sync.WaitGroup
	servantJobTemplate := constants.ConvertServantJobTemplate
	if !convert {
		servantJobTemplate = constants.RevertServantJobTemplate
	}
	for _, nodeName := range edgeNodeNames {
		action, exist := tmplCtx["action"]
		if !exist {
			return errors.New("action is not specified")
		}

		switch action {
		case "convert":
			tmplCtx["jobName"] = ConvertJobNameBase + "-" + nodeName
		case "revert":
			tmplCtx["jobName"] = RevertJobNameBase + "-" + nodeName
		default:
			return fmt.Errorf("unknown action: %s", action)
		}
		tmplCtx["nodeName"] = nodeName

		jobYaml, err := tmplutil.SubsituteTemplate(servantJobTemplate, tmplCtx)
		if err != nil {
			return err
		}
		srvJobObj, err := YamlToObject([]byte(jobYaml))
		if err != nil {
			return err
		}
		srvJob, ok := srvJobObj.(*batchv1.Job)
		if !ok {
			return errors.New("fail to assert yurtctl-servant job")
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := RunJobAndCleanup(cliSet, srvJob,
				WaitServantJobTimeout, CheckServantJobPeriod); err != nil {
				klog.Errorf("fail to run servant job(%s): %s",
					srvJob.GetName(), err)
			} else {
				klog.Infof("servant job(%s) has succeeded", srvJob.GetName())
			}
		}()
	}
	wg.Wait()
	return nil
}

// ValidateServerVersion checks if the target server's version is supported
func ValidateServerVersion(cliSet *kubernetes.Clientset) error {
	serverVersion, err := discovery.
		NewDiscoveryClient(cliSet.RESTClient()).ServerVersion()
	if err != nil {
		return err
	}
	completeVersion := serverVersion.Major + "." + serverVersion.Minor
	if !strutil.IsInStringLst(ValidServerVersions, completeVersion) {
		return fmt.Errorf("server version(%s) is not supported, valid server versions are %v",
			completeVersion, ValidServerVersions)
	}
	return nil
}

// GenClientSet generates the clientset based on command option, environment variable or
// the default kubeconfig file
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
			kbCfgPath = filepath.Join(home, ".kube", "config")
		}
	}

	if kbCfgPath == "" {
		return "", errors.New("either '--kubeconfig', '$HOME/.kube/config' or '$KUBECONFIG' need to be set")
	}

	return kbCfgPath, nil
}

func GetOrCreateJoinTokenString(cliSet *kubernetes.Clientset) (string, error) {
	tokenSelector := fields.SelectorFromSet(
		map[string]string{
			// TODO: We hard-code "type" here until `field_constants.go` that is
			// currently in `pkg/apis/core/` exists in the external API, i.e.
			// k8s.io/api/v1. Should be v1.SecretTypeField
			"type": string(bootstrapapi.SecretTypeBootstrapToken),
		},
	)
	listOptions := metav1.ListOptions{
		FieldSelector: tokenSelector.String(),
	}
	klog.V(1).Infoln("[token] retrieving list of bootstrap tokens")
	secrets, err := cliSet.CoreV1().Secrets(metav1.NamespaceSystem).List(context.Background(), listOptions)
	if err != nil {
		return "", fmt.Errorf("%v%s", err, "failed to list bootstrap tokens")
	}

	for _, secret := range secrets.Items {

		// Get the BootstrapToken struct representation from the Secret object
		token, err := kubeadmapi.BootstrapTokenFromSecret(&secret)
		if err != nil {
			klog.Warningf("%v", err)
			continue
		}
		return token.Token.String(), nil
		// Get the human-friendly string representation for the token
	}

	tokenStr, err := bootstraputil.GenerateBootstrapToken()
	if err != nil {
		return "", fmt.Errorf("couldn't generate random token, %v", err)
	}
	token, err := kubeadmapi.NewBootstrapTokenString(tokenStr)
	if err != nil {
		return "", err
	}

	klog.V(1).Infoln("[token] creating token")
	if err := tokenphase.CreateNewTokens(cliSet, []kubeadmapi.BootstrapToken{{Token: token, Usages: kubeadmcontants.DefaultTokenUsages, Groups: kubeadmcontants.DefaultTokenGroups}}); err != nil {
		return "", err
	}
	return tokenStr, nil
}
