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

package options

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/openyurtio/openyurt/cmd/yurt-tunnel-server/app/config"
	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/constants"
	kubeutil "github.com/openyurtio/openyurt/pkg/yurttunnel/kubernetes"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/pki"

	"github.com/spf13/pflag"
	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server"
)

// ServerOptions has the information that required by the yurttunel-server
type ServerOptions struct {
	KubeConfig             string
	BindAddr               string
	InsecureBindAddr       string
	CertDNSNames           string
	CertIPs                string
	Version                bool
	EnableIptables         bool
	EnableDNSController    bool
	EgressSelectorEnabled  bool
	IptablesSyncPeriod     int
	DNSSyncPeriod          int
	TunnelAgentConnectPort string
	SecurePort             string
	InsecurePort           string
	MetaPort               string
	ServerCount            int
	ProxyStrategy          string
}

// NewServerOptions creates a new ServerOptions
func NewServerOptions() *ServerOptions {
	o := &ServerOptions{
		BindAddr:               "0.0.0.0",
		InsecureBindAddr:       "127.0.0.1",
		EnableIptables:         true,
		EnableDNSController:    true,
		IptablesSyncPeriod:     60,
		DNSSyncPeriod:          1800,
		ServerCount:            1,
		TunnelAgentConnectPort: constants.YurttunnelServerAgentPort,
		SecurePort:             constants.YurttunnelServerMasterPort,
		InsecurePort:           constants.YurttunnelServerMasterInsecurePort,
		MetaPort:               constants.YurttunnelServerMetaPort,
		ProxyStrategy:          string(server.ProxyStrategyDestHost),
	}
	return o
}

// Validate validates the YurttunnelServerOptions
func (o *ServerOptions) Validate() error {
	if len(o.BindAddr) == 0 {
		return fmt.Errorf("%s's bind address can't be empty",
			projectinfo.GetServerName())
	}
	return nil
}

// AddFlags returns flags for a specific yurttunnel-agent by section name
func (o *ServerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.Version, "version", o.Version, fmt.Sprintf("print the version information of the %s.", projectinfo.GetServerName()))
	fs.StringVar(&o.KubeConfig, "kube-config", o.KubeConfig, "path to the kubeconfig file.")
	fs.StringVar(&o.BindAddr, "bind-address", o.BindAddr, fmt.Sprintf("the ip address on which the %s will listen for --secure-port or --tunnel-agent-connect-port port.", projectinfo.GetServerName()))
	fs.StringVar(&o.InsecureBindAddr, "insecure-bind-address", o.InsecureBindAddr, fmt.Sprintf("the ip address on which the %s will listen for --insecure-port port.", projectinfo.GetServerName()))
	fs.StringVar(&o.CertDNSNames, "cert-dns-names", o.CertDNSNames, "DNS names that will be added into server's certificate. (e.g., dns1,dns2)")
	fs.StringVar(&o.CertIPs, "cert-ips", o.CertIPs, "IPs that will be added into server's certificate. (e.g., ip1,ip2)")
	fs.BoolVar(&o.EnableIptables, "enable-iptables", o.EnableIptables, "If allow iptable manager to set the dnat rule.")
	fs.BoolVar(&o.EnableDNSController, "enable-dns-controller", o.EnableDNSController, "If allow DNS controller to set the dns rules.")
	fs.BoolVar(&o.EgressSelectorEnabled, "egress-selector-enable", o.EgressSelectorEnabled, "If the apiserver egress selector has been enabled.")
	fs.IntVar(&o.IptablesSyncPeriod, "iptables-sync-period", o.IptablesSyncPeriod, "The synchronization period of the iptable manager.")
	fs.IntVar(&o.DNSSyncPeriod, "dns-sync-period", o.DNSSyncPeriod, "The synchronization period of the DNS controller.")
	fs.IntVar(&o.ServerCount, "server-count", o.ServerCount, "The number of proxy server instances, should be 1 unless it is an HA server.")
	fs.StringVar(&o.ProxyStrategy, "proxy-strategy", o.ProxyStrategy, "The strategy of proxying requests from tunnel server to agent.")
	fs.StringVar(&o.TunnelAgentConnectPort, "tunnel-agent-connect-port", o.TunnelAgentConnectPort, "The port on which to serve tcp packets from tunnel agent")
	fs.StringVar(&o.SecurePort, "secure-port", o.SecurePort, "The port on which to serve HTTPS requests from cloud clients like prometheus")
	fs.StringVar(&o.InsecurePort, "insecure-port", o.InsecurePort, "The port on which to serve HTTP requests from cloud clients like metrics-server")
	fs.StringVar(&o.MetaPort, "meta-port", o.MetaPort, "The port on which to serve HTTP requests like profling, metrics")
}

func (o *ServerOptions) Config() (*config.Config, error) {
	var err error
	cfg := &config.Config{
		EgressSelectorEnabled: o.EgressSelectorEnabled,
		EnableIptables:        o.EnableIptables,
		EnableDNSController:   o.EnableDNSController,
		IptablesSyncPeriod:    o.IptablesSyncPeriod,
		DNSSyncPeriod:         o.DNSSyncPeriod,
		CertDNSNames:          make([]string, 0),
		CertIPs:               make([]net.IP, 0),
		ServerCount:           o.ServerCount,
		ProxyStrategy:         o.ProxyStrategy,
	}

	if o.CertDNSNames != "" {
		for _, name := range strings.Split(o.CertDNSNames, ",") {
			cfg.CertDNSNames = append(cfg.CertDNSNames, name)
		}
	}

	if o.CertIPs != "" {
		for _, ipStr := range strings.Split(o.CertIPs, ",") {
			ip := net.ParseIP(ipStr)
			if ip != nil {
				cfg.CertIPs = append(cfg.CertIPs, ip)
			}
		}
	}

	cfg.ListenAddrForAgent = net.JoinHostPort(o.BindAddr, o.TunnelAgentConnectPort)
	cfg.ListenAddrForMaster = net.JoinHostPort(o.BindAddr, o.SecurePort)
	cfg.ListenInsecureAddrForMaster = net.JoinHostPort(o.InsecureBindAddr, o.InsecurePort)
	cfg.ListenMetaAddr = net.JoinHostPort(o.InsecureBindAddr, o.MetaPort)
	cfg.RootCert, err = pki.GenRootCertPool(o.KubeConfig, constants.YurttunnelCAFile)
	if err != nil {
		return nil, fmt.Errorf("fail to generate the rootCertPool: %s", err)
	}

	// function 'kubeutil.CreateClientSet' will try to create the clientset
	// based on the in-cluster config if the kubeconfig is empty. As
	// yurttunnel-server will run on the cloud, the in-cluster config should
	// be available.
	cfg.Client, err = kubeutil.CreateClientSet(o.KubeConfig)
	if err != nil {
		return nil, err
	}
	cfg.SharedInformerFactory = informers.NewSharedInformerFactory(cfg.Client, 24*time.Hour)

	klog.Infof("yurttunnel server config: %#+v", cfg)
	return cfg, nil
}
