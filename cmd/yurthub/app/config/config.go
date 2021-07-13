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

package config

import (
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/openyurtio/openyurt/cmd/yurthub/app/options"
	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurthub/cachemanager"
	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/serializer"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage/factory"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
)

// YurtHubConfiguration represents configuration of yurthub
type YurtHubConfiguration struct {
	LBMode                            string
	RemoteServers                     []*url.URL
	YurtHubServerAddr                 string
	YurtHubProxyServerAddr            string
	YurtHubProxyServerSecureAddr      string
	YurtHubProxyServerDummyAddr       string
	YurtHubProxyServerSecureDummyAddr string
	GCFrequency                       int
	CertMgrMode                       string
	NodeName                          string
	HeartbeatFailedRetry              int
	HeartbeatHealthyThreshold         int
	HeartbeatTimeoutSeconds           int
	MaxRequestInFlight                int
	JoinToken                         string
	RootDir                           string
	EnableProfiling                   bool
	EnableDummyIf                     bool
	EnableIptables                    bool
	HubAgentDummyIfName               string
	StorageWrapper                    cachemanager.StorageWrapper
	SerializerManager                 *serializer.SerializerManager
	CertFile                          string
	KeyFile                           string
	KubeConfig                        string
	RootCert                          *x509.CertPool
	Client                            *kubernetes.Clientset
	CertDNSNames                      []string
	CertIPs                           []net.IP
	SharedInformerFactory             informers.SharedInformerFactory
}

// Complete converts *options.YurtHubOptions to *YurtHubConfiguration
func Complete(options *options.YurtHubOptions) (*YurtHubConfiguration, error) {
	us, err := parseRemoteServers(options.ServerAddr)
	if err != nil {
		return nil, err
	}

	storageManager, err := factory.CreateStorage(options.DiskCachePath)
	if err != nil {
		klog.Errorf("could not create storage manager, %v", err)
		return nil, err
	}
	storageWrapper := cachemanager.NewStorageWrapper(storageManager)
	serializerManager := serializer.NewSerializerManager()

	hubServerAddr := net.JoinHostPort(options.YurtHubHost, options.YurtHubPort)
	proxyServerAddr := net.JoinHostPort(options.YurtHubHost, options.YurtHubProxyPort)
	proxySecureServerAddr := net.JoinHostPort(options.YurtHubHost, options.YurtHubProxySecurePort)
	proxyServerDummyAddr := net.JoinHostPort(options.HubAgentDummyIfIP, options.YurtHubProxyPort)
	proxySecureServerDummyAddr := net.JoinHostPort(options.HubAgentDummyIfIP, options.YurtHubProxySecurePort)
	cfg := &YurtHubConfiguration{
		LBMode:                            options.LBMode,
		RemoteServers:                     us,
		YurtHubServerAddr:                 hubServerAddr,
		YurtHubProxyServerAddr:            proxyServerAddr,
		YurtHubProxyServerSecureAddr:      proxySecureServerAddr,
		YurtHubProxyServerDummyAddr:       proxyServerDummyAddr,
		YurtHubProxyServerSecureDummyAddr: proxySecureServerDummyAddr,
		GCFrequency:                       options.GCFrequency,
		CertMgrMode:                       options.CertMgrMode,
		NodeName:                          options.NodeName,
		HeartbeatFailedRetry:              options.HeartbeatFailedRetry,
		HeartbeatHealthyThreshold:         options.HeartbeatHealthyThreshold,
		HeartbeatTimeoutSeconds:           options.HeartbeatTimeoutSeconds,
		MaxRequestInFlight:                options.MaxRequestInFlight,
		JoinToken:                         options.JoinToken,
		RootDir:                           options.RootDir,
		EnableProfiling:                   options.EnableProfiling,
		EnableDummyIf:                     options.EnableDummyIf,
		EnableIptables:                    options.EnableIptables,
		HubAgentDummyIfName:               options.HubAgentDummyIfName,
		StorageWrapper:                    storageWrapper,
		SerializerManager:                 serializerManager,
	}

	return cfg, nil
}

func parseRemoteServers(serverAddr string) ([]*url.URL, error) {
	servers := strings.Split(serverAddr, ",")
	us := make([]*url.URL, 0, len(servers))
	remoteServers := make([]string, 0, len(servers))
	for _, server := range servers {
		u, err := url.Parse(server)
		if err != nil {
			klog.Errorf("failed to parse server address %s, %v", servers, err)
			return us, err
		}
		if u.Scheme == "" {
			u.Scheme = "https"
		} else if u.Scheme != "https" {
			return us, fmt.Errorf("only https scheme is supported for server address(%s)", serverAddr)
		}
		us = append(us, u)
		remoteServers = append(remoteServers, u.String())
	}

	if len(us) < 1 {
		return us, fmt.Errorf("no server address is set, can not connect remote server")
	}
	klog.Infof("%s would connect remote servers: %s", projectinfo.GetHubName(), strings.Join(remoteServers, ","))

	return us, nil
}
