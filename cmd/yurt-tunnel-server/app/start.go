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
	"sync"
	"time"

	"github.com/openyurtio/openyurt/cmd/yurt-tunnel-server/app/config"
	"github.com/openyurtio/openyurt/cmd/yurt-tunnel-server/app/options"
	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/constants"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/dns"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/handlerwrapper/initializer"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/handlerwrapper/wraphandler"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/iptables"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/pki"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/pki/certmanager"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/server"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/util"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// NewYurttunnelServerCommand creates a new yurttunnel-server command
func NewYurttunnelServerCommand(stopCh <-chan struct{}) *cobra.Command {
	serverOptions := options.NewServerOptions()

	cmd := &cobra.Command{
		Use:   "Launch " + projectinfo.GetServerName(),
		Short: projectinfo.GetServerName() + " sends requests to " + projectinfo.GetAgentName(),
		RunE: func(c *cobra.Command, args []string) error {
			if serverOptions.Version {
				fmt.Printf("%s: %#v\n", projectinfo.GetServerName(), projectinfo.Get())
				return nil
			}
			klog.Infof("%s version: %#v", projectinfo.GetServerName(), projectinfo.Get())

			if err := serverOptions.Validate(); err != nil {
				return err
			}

			cfg, err := serverOptions.Config()
			if err != nil {
				return err
			}
			if err := Run(cfg.Complete(), stopCh); err != nil {
				return err
			}
			return nil
		},
	}

	serverOptions.AddFlags(cmd.Flags())

	return cmd
}

// run starts the yurttunel-server
func Run(cfg *config.CompletedConfig, stopCh <-chan struct{}) error {
	var wg sync.WaitGroup
	// 0. start the DNS controller
	if cfg.EnableDNSController {
		dnsController, err := dns.NewCoreDNSRecordController(cfg.Client,
			cfg.SharedInformerFactory,
			cfg.ListenInsecureAddrForMaster,
			cfg.DNSSyncPeriod)
		if err != nil {
			return fmt.Errorf("fail to create a new dnsController, %v", err)
		}
		go dnsController.Run(stopCh)
	}
	// 1. start the IP table manager
	if cfg.EnableIptables {
		iptablesMgr := iptables.NewIptablesManager(cfg.Client,
			cfg.SharedInformerFactory.Core().V1().Nodes(),
			cfg.ListenAddrForMaster,
			cfg.ListenInsecureAddrForMaster,
			cfg.IptablesSyncPeriod)
		if iptablesMgr == nil {
			return fmt.Errorf("fail to create a new IptableManager")
		}
		wg.Add(1)
		go iptablesMgr.Run(stopCh, &wg)
	}

	// 2. create a certificate manager for the tunnel server and run the
	// csr approver for both yurttunnel-server and yurttunnel-agent
	serverCertMgr, err := certmanager.NewYurttunnelServerCertManager(cfg.Client, cfg.CertDNSNames, cfg.CertIPs, stopCh)
	if err != nil {
		return err
	}
	serverCertMgr.Start()
	go certmanager.NewCSRApprover(cfg.Client, cfg.SharedInformerFactory.Certificates().V1beta1().CertificateSigningRequests()).
		Run(constants.YurttunnelCSRApproverThreadiness, stopCh)

	// 3. generate the TLS configuration based on the latest certificate
	tlsCfg, err := pki.GenTLSConfigUseCertMgrAndCertPool(serverCertMgr, cfg.RootCert)
	if err != nil {
		return err
	}

	// 4. create handler wrappers
	mInitializer := initializer.NewMiddlewareInitializer(cfg.SharedInformerFactory)
	wrappers, err := wraphandler.InitHandlerWrappers(mInitializer)
	if err != nil {
		return err
	}

	// after all of informers are configured completed, start the shared index informer
	cfg.SharedInformerFactory.Start(stopCh)

	// 5. waiting for the certificate is generated
	_ = wait.PollUntil(5*time.Second, func() (bool, error) {
		// keep polling until the certificate is signed
		if serverCertMgr.Current() != nil {
			return true, nil
		}
		klog.Infof("waiting for the master to sign the %s certificate", projectinfo.GetServerName())
		return false, nil
	}, stopCh)

	// 6. start the server
	ts := server.NewTunnelServer(
		cfg.EgressSelectorEnabled,
		cfg.InterceptorServerUDSFile,
		cfg.ListenAddrForMaster,
		cfg.ListenInsecureAddrForMaster,
		cfg.ListenAddrForAgent,
		cfg.ServerCount,
		tlsCfg,
		wrappers,
		cfg.ProxyStrategy)
	if err := ts.Run(); err != nil {
		return err
	}

	// 7. start meta server
	util.RunMetaServer(cfg.ListenMetaAddr)

	<-stopCh
	wg.Wait()
	return nil
}
