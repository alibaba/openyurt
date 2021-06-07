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
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/constants"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/server/serveraddr"

	certificates "k8s.io/api/certificates/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clicert "k8s.io/client-go/kubernetes/typed/certificates/v1beta1"
	"k8s.io/client-go/util/certificate"
	"k8s.io/klog/v2"
)

// NewYurttunnelServerCertManager creates a certificate manager for
// the yurttunnel-server
func NewYurttunnelServerCertManager(
	clientset kubernetes.Interface,
	clCertNames []string,
	clIPs []net.IP,
	stopCh <-chan struct{}) (certificate.Manager, error) {
	// get server DNS names and IPs
	var (
		dnsNames = []string{}
		ips      = []net.IP{}
		err      error
	)
	_ = wait.PollUntil(5*time.Second, func() (bool, error) {
		dnsNames, ips, err = serveraddr.GetYurttunelServerDNSandIP(clientset)
		if err != nil {
			return false, err
		}

		// get clusterIP for tunnel server internal service
		svc, err := clientset.CoreV1().Services(constants.YurttunnelServerServiceNs).Get(context.Background(), constants.YurttunnelServerInternalServiceName, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			// compatible with versions that not supported x-tunnel-server-internal-svc
			return true, nil
		} else if err != nil {
			return false, err
		}

		if svc.Spec.ClusterIP != "" && net.ParseIP(svc.Spec.ClusterIP) != nil {
			ips = append(ips, net.ParseIP(svc.Spec.ClusterIP))
			dnsNames = append(dnsNames, serveraddr.GetDefaultDomainsForSvc(svc.Namespace, svc.Name)...)
		}

		return true, nil
	}, stopCh)
	// add user specified DNS anems and IP addresses
	dnsNames = append(dnsNames, clCertNames...)
	ips = append(ips, clIPs...)
	klog.Infof("subject of tunnel server certificate, ips=%#+v, dnsNames=%#+v", ips, dnsNames)

	return newCertManager(
		clientset,
		projectinfo.GetServerName(),
		fmt.Sprintf(constants.YurttunnelServerCertDir, projectinfo.GetServerName()),
		constants.YurttunneServerCSRCN,
		[]string{constants.YurttunneServerCSROrg, constants.YurttunnelCSROrg},
		dnsNames, ips)
}

// NewYurttunnelAgentCertManager creates a certificate manager for
// the yurttunel-agent
func NewYurttunnelAgentCertManager(
	clientset kubernetes.Interface) (certificate.Manager, error) {
	// As yurttunnel-agent will run on the edge node with Host network mode,
	// we can use the status.podIP as the node IP
	nodeIP := os.Getenv(constants.YurttunnelAgentPodIPEnv)
	if nodeIP == "" {
		return nil, fmt.Errorf("env %s is not set",
			constants.YurttunnelAgentPodIPEnv)
	}

	return newCertManager(
		clientset,
		projectinfo.GetAgentName(),
		fmt.Sprintf(constants.YurttunnelAgentCertDir, projectinfo.GetAgentName()),
		constants.YurttunnelAgentCSRCN,
		[]string{constants.YurttunnelCSROrg},
		[]string{os.Getenv("NODE_NAME")},
		[]net.IP{net.ParseIP(nodeIP)})
}

// NewCertManager creates a certificate manager that will generates a
// certificate by sending a csr to the apiserver
func newCertManager(
	clientset kubernetes.Interface,
	componentName,
	certDir,
	commonName string,
	organizations,
	dnsNames []string, ipAddrs []net.IP) (certificate.Manager, error) {
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
			DNSNames:    dnsNames,
			IPAddresses: ipAddrs,
		}
	}

	certManager, err := certificate.NewManager(&certificate.Config{
		ClientFn: func(current *tls.Certificate) (clicert.CertificateSigningRequestInterface, error) {
			return clientset.CertificatesV1beta1().CertificateSigningRequests(), nil
		},
		SignerName:  certificates.LegacyUnknownSignerName,
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
