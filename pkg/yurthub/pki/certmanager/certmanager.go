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

package certmanager

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"

	"github.com/openyurtio/openyurt/pkg/projectinfo"

	certificates "k8s.io/api/certificates/v1beta1"
	"k8s.io/client-go/kubernetes"
	clicert "k8s.io/client-go/kubernetes/typed/certificates/v1beta1"
	"k8s.io/client-go/util/certificate"
	"k8s.io/klog/v2"
)

const (
	YurtHubServerCSROrg = "system:nodes"
	YurtHubServerCSRCN  = "kube-apiserver-kubelet-client"
)

// NewYurtHubServerCertManager creates a certificate manager for
// the yurthub-server
func NewYurtHubServerCertManager(
	clientset kubernetes.Interface,
	certDir string) (certificate.Manager, error) {

	klog.Infof("subject of yurthub server certificate")
	return newCertManager(
		clientset,
		projectinfo.GetHubName(),
		certDir,
		YurtHubServerCSRCN,
		[]string{YurtHubServerCSROrg})
}

// NewCertManager creates a certificate manager that will generates a
// certificate by sending a csr to the apiserver
func newCertManager(
	clientset kubernetes.Interface,
	componentName,
	certDir,
	commonName string,
	organizations []string) (certificate.Manager, error) {
	certificateStore, err :=
		certificate.NewFileStore(componentName, certDir, certDir, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize the server certificate store: %v", err)
	}

	getTemplate := func() *x509.CertificateRequest {
		return &x509.CertificateRequest{
			Subject: pkix.Name{
				CommonName:   commonName,
				Organization: organizations,
			},
		}
	}

	certManager, err := certificate.NewManager(&certificate.Config{
		ClientFn: func(current *tls.Certificate) (clicert.CertificateSigningRequestInterface, error) {
			return clientset.CertificatesV1beta1().CertificateSigningRequests(), nil
		},
		SignerName:  certificates.KubeAPIServerClientKubeletSignerName,
		GetTemplate: getTemplate,
		Usages: []certificates.KeyUsage{
			certificates.UsageAny,
		},
		CertificateStore: certificateStore,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize server certificate manager: %v", err)
	}

	return certManager, nil
}
