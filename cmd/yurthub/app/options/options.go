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

package options

import (
	"fmt"
	"net"
	"path/filepath"

	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage/disk"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"
	"github.com/spf13/pflag"
)

const (
	DummyIfCIDR   = "169.254.0.0/16"
	ExclusiveCIDR = "169.254.31.0/24"
)

// YurtHubOptions is the main settings for the yurthub
type YurtHubOptions struct {
	ServerAddr                string
	YurtHubHost               string
	YurtHubPort               string
	YurtHubProxyPort          string
	YurtHubProxySecurePort    string
	GCFrequency               int
	CertMgrMode               string
	NodeName                  string
	LBMode                    string
	HeartbeatFailedRetry      int
	HeartbeatHealthyThreshold int
	HeartbeatTimeoutSeconds   int
	MaxRequestInFlight        int
	JoinToken                 string
	RootDir                   string
	Version                   bool
	EnableProfiling           bool
	EnableDummyIf             bool
	EnableIptables            bool
	HubAgentDummyIfIP         string
	HubAgentDummyIfName       string
	DiskCachePath             string
}

// NewYurtHubOptions creates a new YurtHubOptions with a default config.
func NewYurtHubOptions() *YurtHubOptions {
	o := &YurtHubOptions{
		YurtHubHost:               "127.0.0.1",
		YurtHubProxySecurePort:    "10260",
		YurtHubProxyPort:          "10261",
		YurtHubPort:               "10267",
		GCFrequency:               120,
		CertMgrMode:               "hubself",
		LBMode:                    "rr",
		HeartbeatFailedRetry:      3,
		HeartbeatHealthyThreshold: 2,
		HeartbeatTimeoutSeconds:   2,
		MaxRequestInFlight:        250,
		RootDir:                   filepath.Join("/var/lib/", projectinfo.GetHubName()),
		EnableProfiling:           true,
		EnableDummyIf:             true,
		EnableIptables:            true,
		HubAgentDummyIfIP:         "169.254.2.1",
		HubAgentDummyIfName:       fmt.Sprintf("%s-dummy0", projectinfo.GetHubName()),
		DiskCachePath:             disk.CacheBaseDir,
	}

	return o
}

// ValidateOptions validates YurtHubOptions
func ValidateOptions(options *YurtHubOptions) error {
	if len(options.NodeName) == 0 {
		return fmt.Errorf("node name is empty")
	}

	if len(options.ServerAddr) == 0 {
		return fmt.Errorf("server-address is empty")
	}

	if !util.IsSupportedLBMode(options.LBMode) {
		return fmt.Errorf("lb mode(%s) is not supported", options.LBMode)
	}

	if !util.IsSupportedCertMode(options.CertMgrMode) {
		return fmt.Errorf("cert manage mode %s is not supported", options.CertMgrMode)
	}

	if err := verifyDummyIP(options.HubAgentDummyIfIP); err != nil {
		return fmt.Errorf("dummy ip %s is not invalid, %v", options.HubAgentDummyIfIP, err)
	}

	return nil
}

// AddFlags returns flags for a specific yurthub by section name
func (o *YurtHubOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.YurtHubHost, "bind-address", o.YurtHubHost, "the IP address on which to listen for the --serve-port port.")
	fs.StringVar(&o.YurtHubPort, "serve-port", o.YurtHubPort, "the port on which to serve HTTP requests(like profiling, metrics) for hub agent.")
	fs.StringVar(&o.YurtHubProxyPort, "proxy-port", o.YurtHubProxyPort, "the port on which to proxy HTTP requests to kube-apiserver")
	fs.StringVar(&o.YurtHubProxySecurePort, "proxy-secure-port", o.YurtHubProxySecurePort, "the port on which to proxy HTTPS requests to kube-apiserver")
	fs.StringVar(&o.ServerAddr, "server-addr", o.ServerAddr, "the address of Kubernetes kube-apiserver,the format is: \"server1,server2,...\"")
	fs.StringVar(&o.CertMgrMode, "cert-mgr-mode", o.CertMgrMode, "the cert manager mode, kubelet: use certificates that belongs to kubelet, hubself: auto generate client cert for hub agent.")
	fs.IntVar(&o.GCFrequency, "gc-frequency", o.GCFrequency, "the frequency to gc cache in storage(unit: minute).")
	fs.StringVar(&o.NodeName, "node-name", o.NodeName, "the name of node that runs hub agent")
	fs.StringVar(&o.LBMode, "lb-mode", o.LBMode, "the mode of load balancer to connect remote servers(rr, priority)")
	fs.IntVar(&o.HeartbeatFailedRetry, "heartbeat-failed-retry", o.HeartbeatFailedRetry, "number of heartbeat request retry after having failed.")
	fs.IntVar(&o.HeartbeatHealthyThreshold, "heartbeat-healthy-threshold", o.HeartbeatHealthyThreshold, "minimum consecutive successes for the heartbeat to be considered healthy after having failed.")
	fs.IntVar(&o.HeartbeatTimeoutSeconds, "heartbeat-timeout-seconds", o.HeartbeatTimeoutSeconds, " number of seconds after which the heartbeat times out.")
	fs.IntVar(&o.MaxRequestInFlight, "max-requests-in-flight", o.MaxRequestInFlight, "the maximum number of parallel requests.")
	fs.StringVar(&o.JoinToken, "join-token", o.JoinToken, "the Join token for bootstrapping hub agent when --cert-mgr-mode=hubself.")
	fs.StringVar(&o.RootDir, "root-dir", o.RootDir, "directory path for managing hub agent files(pki, cache etc).")
	fs.BoolVar(&o.Version, "version", o.Version, "print the version information.")
	fs.BoolVar(&o.EnableProfiling, "profiling", o.EnableProfiling, "Enable profiling via web interface host:port/debug/pprof/")
	fs.BoolVar(&o.EnableDummyIf, "enable-dummy-if", o.EnableDummyIf, "enable dummy interface or not")
	fs.BoolVar(&o.EnableIptables, "enable-iptables", o.EnableIptables, "enable iptables manager to setup rules for accessing hub agent")
	fs.StringVar(&o.HubAgentDummyIfIP, "dummy-if-ip", o.HubAgentDummyIfIP, "the ip address of dummy interface that used for container connect hub agent(exclusive ips: 169.254.31.0/24, 169.254.1.1/32)")
	fs.StringVar(&o.HubAgentDummyIfName, "dummy-if-name", o.HubAgentDummyIfName, "the name of dummy interface that is used for hub agent")
	fs.StringVar(&o.DiskCachePath, "disk-cache-path", o.DiskCachePath, "the path for kubernetes to storage metadata")
}

// verifyDummyIP verify the specified ip is valid or not
func verifyDummyIP(dummyIP string) error {
	//169.254.2.1/32
	dip := net.ParseIP(dummyIP)
	if dip == nil {
		return fmt.Errorf("dummy ip %s is invalid", dummyIP)
	}

	_, dummyIfIPNet, err := net.ParseCIDR(DummyIfCIDR)
	if err != nil {
		return fmt.Errorf("cidr(%s) is invalid, %v", DummyIfCIDR, err)
	}

	if !dummyIfIPNet.Contains(dip) {
		return fmt.Errorf("dummy ip %s is not in cidr(%s)", dummyIP, DummyIfCIDR)
	}

	_, exclusiveIPNet, err := net.ParseCIDR(ExclusiveCIDR)
	if err != nil {
		return fmt.Errorf("cidr(%s) is invalid, %v", ExclusiveCIDR, err)
	}

	if exclusiveIPNet.Contains(dip) {
		return fmt.Errorf("dummy ip %s is in reserved cidr(%s)", dummyIP, ExclusiveCIDR)
	}

	if dummyIP == "169.254.1.1" {
		return fmt.Errorf("dummy ip is a reserved ip(%s)", dummyIP)
	}

	return nil
}
