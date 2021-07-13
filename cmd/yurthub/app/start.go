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

package app

import (
	"fmt"

	"github.com/openyurtio/openyurt/cmd/yurthub/app/config"
	"github.com/openyurtio/openyurt/cmd/yurthub/app/options"
	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurthub/cachemanager"
	"github.com/openyurtio/openyurt/pkg/yurthub/certificate"
	"github.com/openyurtio/openyurt/pkg/yurthub/certificate/hubself"
	"github.com/openyurtio/openyurt/pkg/yurthub/certificate/kubelet"
	"github.com/openyurtio/openyurt/pkg/yurthub/gc"
	"github.com/openyurtio/openyurt/pkg/yurthub/healthchecker"
	"github.com/openyurtio/openyurt/pkg/yurthub/network"
	"github.com/openyurtio/openyurt/pkg/yurthub/proxy"
	"github.com/openyurtio/openyurt/pkg/yurthub/server"
	"github.com/openyurtio/openyurt/pkg/yurthub/transport"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/klog"
)

// NewCmdStartYurtHub creates a *cobra.Command object with default parameters
func NewCmdStartYurtHub(stopCh <-chan struct{}) *cobra.Command {
	yurtHubOptions := options.NewYurtHubOptions()

	cmd := &cobra.Command{
		Use:   projectinfo.GetHubName(),
		Short: "Launch " + projectinfo.GetHubName(),
		Long:  "Launch " + projectinfo.GetHubName(),
		Run: func(cmd *cobra.Command, args []string) {
			if yurtHubOptions.Version {
				fmt.Printf("%s: %#v\n", projectinfo.GetHubName(), projectinfo.Get())
				return
			}
			fmt.Printf("%s version: %#v\n", projectinfo.GetHubName(), projectinfo.Get())

			cmd.Flags().VisitAll(func(flag *pflag.Flag) {
				klog.V(1).Infof("FLAG: --%s=%q", flag.Name, flag.Value)
			})
			if err := options.ValidateOptions(yurtHubOptions); err != nil {
				klog.Fatalf("validate options: %v", err)
			}

			yurtHubCfg, err := config.Complete(yurtHubOptions)
			if err != nil {
				klog.Fatalf("complete %s configuration error, %v", projectinfo.GetHubName(), err)
			}
			klog.Infof("%s cfg: %#+v", projectinfo.GetHubName(), yurtHubCfg)

			if err := Run(yurtHubCfg, stopCh); err != nil {
				klog.Fatalf("run %s failed, %v", projectinfo.GetHubName(), err)
			}
		},
	}

	yurtHubOptions.AddFlags(cmd.Flags())
	return cmd
}

// Run runs the YurtHubConfiguration. This should never exit
func Run(cfg *config.YurtHubConfiguration, stopCh <-chan struct{}) error {
	trace := 1
	klog.Infof("%d. register cert managers", trace)
	cmr := certificate.NewCertificateManagerRegistry()
	kubelet.Register(cmr)
	hubself.Register(cmr)
	trace++

	klog.Infof("%d. create cert manager with %s mode", trace, cfg.CertMgrMode)
	certManager, err := cmr.New(cfg.CertMgrMode, cfg)
	if err != nil {
		klog.Errorf("could not create certificate manager, %v", err)
		return err
	}
	trace++

	klog.Infof("%d. new transport manager", trace)
	transportManager, err := transport.NewTransportManager(certManager, stopCh)
	if err != nil {
		klog.Errorf("could not new transport manager, %v", err)
		return err
	}
	trace++

	klog.Infof("%d. create health checker for remote servers ", trace)
	healthChecker, err := healthchecker.NewHealthChecker(cfg, transportManager, stopCh)
	if err != nil {
		klog.Errorf("could not new health checker, %v", err)
		return err
	}
	healthChecker.Run()
	trace++

	klog.Infof("%d. new cache manager with storage wrapper and serializer manager", trace)
	cacheMgr, err := cachemanager.NewCacheManager(cfg.StorageWrapper, cfg.SerializerManager)
	if err != nil {
		klog.Errorf("could not new cache manager, %v", err)
		return err
	}
	trace++

	klog.Infof("%d. new gc manager for node %s, and gc frequency is a random time between %d min and %d min", trace, cfg.NodeName, cfg.GCFrequency, 3*cfg.GCFrequency)
	gcMgr, err := gc.NewGCManager(cfg, transportManager, stopCh)
	if err != nil {
		klog.Errorf("could not new gc manager, %v", err)
		return err
	}
	gcMgr.Run()
	trace++

	klog.Infof("%d. new reverse proxy handler for remote servers", trace)
	yurtProxyHandler, err := proxy.NewYurtReverseProxyHandler(cfg, cacheMgr, transportManager, healthChecker, certManager, stopCh)
	if err != nil {
		klog.Errorf("could not create reverse proxy handler, %v", err)
		return err
	}
	trace++

	if cfg.EnableDummyIf {
		klog.Infof("%d. create dummy network interface %s and init iptables manager", trace, cfg.HubAgentDummyIfName)
		networkMgr, err := network.NewNetworkManager(cfg)
		if err != nil {
			klog.Errorf("could not create network manager, %v", err)
			return err
		}
		networkMgr.Run(stopCh)
		trace++
		klog.Infof("%d. new %s server and begin to serve, dummy proxy server: %s", trace, projectinfo.GetHubName(), cfg.YurtHubProxyServerDummyAddr)
	}

	klog.Infof("%d. new %s server and begin to serve, proxy server: %s, secure proxy server: %s, hub server: %s", trace, projectinfo.GetHubName(), cfg.YurtHubProxyServerAddr, cfg.YurtHubProxyServerSecureAddr, cfg.YurtHubServerAddr)
	s, err := server.NewYurtHubServer(cfg, certManager, yurtProxyHandler, stopCh)
	if err != nil {
		klog.Errorf("could not create hub server, %v", err)
		return err
	}
	s.Run()

	klog.Infof("hub agent exited")
	<-stopCh
	return nil
}
