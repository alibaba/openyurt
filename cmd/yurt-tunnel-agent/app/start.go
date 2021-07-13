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
	"time"

	"github.com/openyurtio/openyurt/cmd/yurt-tunnel-agent/app/config"
	"github.com/openyurtio/openyurt/cmd/yurt-tunnel-agent/app/options"
	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/agent"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/constants"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/pki"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/pki/certmanager"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/server/serveraddr"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/util"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/certificate"
	"k8s.io/klog/v2"
)

// NewYurttunnelAgentCommand creates a new yurttunnel-agent command
func NewYurttunnelAgentCommand(stopCh <-chan struct{}) *cobra.Command {
	agentOptions := options.NewAgentOptions()

	cmd := &cobra.Command{
		Short: fmt.Sprintf("Launch %s", projectinfo.GetAgentName()),
		RunE: func(c *cobra.Command, args []string) error {
			if agentOptions.Version {
				fmt.Printf("%s: %#v\n", projectinfo.GetAgentName(), projectinfo.Get())
				return nil
			}
			klog.Infof("%s version: %#v", projectinfo.GetAgentName(), projectinfo.Get())

			if err := agentOptions.Validate(); err != nil {
				return err
			}

			cfg, err := agentOptions.Config()
			if err != nil {
				return err
			}
			if err := Run(cfg.Complete(), stopCh); err != nil {
				return err
			}
			return nil
		},
	}

	agentOptions.AddFlags(cmd.Flags())
	return cmd
}

// Run starts the yurttunel-agent
func Run(cfg *config.CompletedConfig, stopCh <-chan struct{}) error {
	var (
		tunnelServerAddr string
		err              error
		agentCertMgr     certificate.Manager
	)

	// 1. get the address of the yurttunnel-server
	tunnelServerAddr = cfg.TunnelServerAddr
	if tunnelServerAddr == "" {
		if tunnelServerAddr, err = serveraddr.GetTunnelServerAddr(cfg.Client); err != nil {
			return err
		}
	}
	klog.Infof("%s address: %s", projectinfo.GetServerName(), tunnelServerAddr)

	// 2. create a certificate manager
	agentCertMgr, err =
		certmanager.NewYurttunnelAgentCertManager(cfg.Client)
	if err != nil {
		return err
	}
	agentCertMgr.Start()

	// 2.1. waiting for the certificate is generated
	_ = wait.PollUntil(5*time.Second, func() (bool, error) {
		if agentCertMgr.Current() != nil {
			return true, nil
		}
		klog.Infof("certificate %s not signed, waiting...",
			projectinfo.GetAgentName())
		return false, nil
	}, stopCh)
	klog.Infof("certificate %s ok", projectinfo.GetAgentName())

	// 3. generate a TLS configuration for securing the connection to server
	tlsCfg, err := pki.GenTLSConfigUseCertMgrAndCA(agentCertMgr,
		tunnelServerAddr, constants.YurttunnelCAFile)
	if err != nil {
		return err
	}

	// 4. start the yurttunnel-agent
	ta := agent.NewTunnelAgent(tlsCfg, tunnelServerAddr, cfg.NodeName, cfg.AgentIdentifiers)
	ta.Run(stopCh)

	// 5. start meta server
	util.RunMetaServer(cfg.AgentMetaAddr)

	<-stopCh
	return nil
}
