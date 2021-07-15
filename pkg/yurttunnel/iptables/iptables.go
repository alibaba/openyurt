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

package iptables

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	coreinformer "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/iptables"
	"k8s.io/utils/exec"
	utilnet "k8s.io/utils/net"

	"github.com/openyurtio/openyurt/pkg/projectinfo"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/server/metrics"
	"github.com/openyurtio/openyurt/pkg/yurttunnel/util"
)

const (
	kubeletSecurePort         = "10250"
	kubeletInsecurePort       = "10255"
	loopbackAddr              = "127.0.0.1"
	reqReturnComment          = "return request to access node directly"
	dnatToTunnelComment       = "dnat to tunnel for access node"
	yurttunnelServerPortChain = "TUNNEL-PORT"
	yurttunnelPortChainPrefix = "TUNNEL-PORT-"
	defaultSyncPeriod         = 15
)

var (
	tunnelCommentStr   = strings.ReplaceAll(projectinfo.GetTunnelName(), "-", " ")
	iptablesJumpChains = []iptablesJumpChain{
		{
			table:     iptables.TableNAT,
			dstChain:  yurttunnelServerPortChain,
			srcChain:  iptables.ChainOutput,
			comment:   fmt.Sprintf("%s server port", tunnelCommentStr),
			extraArgs: []string{"-p", "tcp"},
		},
	}
)

type iptablesJumpChain struct {
	table     iptables.Table
	dstChain  iptables.Chain
	srcChain  iptables.Chain
	comment   string
	extraArgs []string
}

// IptableManager interface defines the method for adding dnat rules to host
// that needs to send network packages to kubelets
type IptablesManager interface {
	Run(stopCh <-chan struct{}, wg *sync.WaitGroup)
}

// iptablesManager implements the IptablesManager
type iptablesManager struct {
	kubeClient       clientset.Interface
	nodeInformer     coreinformer.NodeInformer
	iptables         iptables.Interface
	execer           exec.Interface
	conntrackPath    string
	secureDnatDest   string
	insecureDnatDest string
	insecurePort     string
	lastNodesIP      []string
	lastDnatPorts    []string
	syncPeriod       int
}

// NewIptablesManager creates an IptablesManager; deletes old chains, if any;
// generates new dnat rules based on IPs of current active nodes; and
// appends the rules to the iptable.
func NewIptablesManager(client clientset.Interface,
	nodeInformer coreinformer.NodeInformer,
	listenAddr string,
	listenInsecureAddr string,
	syncPeriod int) IptablesManager {

	protocol := iptables.ProtocolIpv4
	execer := exec.New()
	iptInterface := iptables.New(execer, protocol)

	if syncPeriod < defaultSyncPeriod {
		syncPeriod = defaultSyncPeriod
	}

	_, insecurePort, err := net.SplitHostPort(listenInsecureAddr)
	if err != nil {
		return nil
	}

	im := &iptablesManager{
		kubeClient:       client,
		iptables:         iptInterface,
		execer:           execer,
		nodeInformer:     nodeInformer,
		secureDnatDest:   listenAddr,
		insecureDnatDest: listenInsecureAddr,
		insecurePort:     insecurePort,
		lastNodesIP:      make([]string, 0),
		lastDnatPorts:    make([]string, 0),
		syncPeriod:       syncPeriod,
	}

	// conntrack setting
	conntrackPath, err := im.execer.LookPath("conntrack")
	if err != nil {
		klog.Errorf("error looking for path of conntrack: %v", err)
	} else {
		im.conntrackPath = conntrackPath
	}

	// 1. if there exist any old jump chain, delete them
	_ = im.deleteJumpChains(iptablesJumpChains)

	return im
}

// Run starts the iptablesManager that will updates dnat rules periodically
func (im *iptablesManager) Run(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	// wait the nodeInformer has synced
	if !cache.WaitForCacheSync(stopCh,
		im.nodeInformer.Informer().HasSynced) {
		klog.Error("sync node cache timeout")
		return
	}
	// sync iptables setting when tunnel server startup
	im.syncIptableSetting()

	ticker := time.NewTicker(time.Duration(im.syncPeriod) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			klog.Info("stop the iptablesManager")
			im.cleanupIptableSetting()
			return
		case <-ticker.C:
			im.syncIptableSetting()
		}
	}
}

func (im *iptablesManager) cleanupIptableSetting() {
	dnatPorts, err := util.GetConfiguredDnatPorts(im.kubeClient, im.insecurePort)
	if err != nil {
		klog.Errorf("failed to cleanup iptables rules, %v", err)
		return
	}
	_, deletedDnatPorts := im.getDeletedPorts(dnatPorts)
	currentDnatPorts := append(dnatPorts, kubeletSecurePort, kubeletInsecurePort)
	deletedJumpChains := []iptables.Chain{iptablesJumpChains[0].dstChain}
	for _, port := range deletedDnatPorts {
		deletedJumpChains = append(deletedJumpChains, iptables.Chain(fmt.Sprintf("%s%s", yurttunnelPortChainPrefix, port)))
	}
	for _, port := range currentDnatPorts {
		deletedJumpChains = append(deletedJumpChains, iptables.Chain(fmt.Sprintf("%s%s", yurttunnelPortChainPrefix, port)))
	}

	args := append(iptablesJumpChains[0].extraArgs, "-m", "comment", "--comment",
		iptablesJumpChains[0].comment, "-j", string(iptablesJumpChains[0].dstChain))
	if err := im.iptables.DeleteRule(iptablesJumpChains[0].table, iptablesJumpChains[0].srcChain, args...); err != nil {
		klog.Errorf("failed to delete rule that %s chain %s jumps to %s: %v",
			iptablesJumpChains[0].table, iptablesJumpChains[0].srcChain, iptablesJumpChains[0].dstChain, err)
	}
	im.deleteJumpChainsWithoutCheck(deletedJumpChains)
	klog.Info("Complete cleanup iptables rules")
}

func (im *iptablesManager) deleteJumpChainsWithoutCheck(Chains []iptables.Chain) {
	table := iptables.TableNAT
	for _, chain := range Chains {
		if err := im.iptables.FlushChain(table, chain); err != nil {
			klog.Errorf("could not flush %s chain %s: %v",
				table, chain, err)
		}
		if err := im.iptables.DeleteChain(table, chain); err != nil {
			klog.Errorf("could not delete %s chain %s: %v",
				table, chain, err)
		}
	}
}

func (im *iptablesManager) deleteJumpChains(jumpChains []iptablesJumpChain) error {
	for _, jump := range jumpChains {
		args := append(jump.extraArgs, "-m", "comment", "--comment",
			jump.comment, "-j", string(jump.dstChain))
		// delete the jump rule
		if err := im.iptables.DeleteRule(jump.table, jump.srcChain, args...); err != nil {
			klog.Errorf("failed to delete rule that %s chain %s jumps to %s: %v",
				jump.table, jump.srcChain, jump.dstChain, err)
			return err
		}

		// flush all rules in the destination chain
		if err := im.iptables.FlushChain(jump.table, jump.dstChain); err != nil {
			klog.Errorf("could not flush %s chain %s: %v",
				jump.table, jump.dstChain, err)
			return err
		}

		// delete the destination chain
		if err := im.iptables.DeleteChain(jump.table, jump.dstChain); err != nil {
			klog.Errorf("could not delete %s chain %s: %v",
				jump.table, jump.dstChain, err)
			return err
		}
	}
	return nil
}

// getIPOfNodesWithoutAgent returns the ip addresses of all nodes that
// are not running yurttunnel-agent
func (im *iptablesManager) getIPOfNodesWithoutAgent() []string {
	var nodesIP []string
	nodes, err := im.nodeInformer.Lister().List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list nodes for iptables: %v", err)
		return nodesIP
	}

	for i := range nodes {
		if withoutAgent(nodes[i]) && isNodeReady(nodes[i]) {
			nodeIPs := getNodeInternalIPs(nodes[i])
			nodesIP = append(nodesIP, nodeIPs...)
		}
	}

	klog.V(4).Infof("nodes without %s: %s", projectinfo.GetAgentName(), strings.Join(nodesIP, ","))
	metrics.Metrics.ObserveCloudNodes(len(nodesIP))
	return nodesIP
}

// withoutAgent used to determine whether the node is running an tunnel agent
func withoutAgent(node *corev1.Node) bool {
	tunnelAgentNode, ok := node.Labels[projectinfo.GetEdgeEnableTunnelLabelKey()]
	if ok && tunnelAgentNode == "true" {
		return false
	}

	edgeNode, ok := node.Labels[projectinfo.GetEdgeWorkerLabelKey()]
	if ok && edgeNode == "true" {
		return false
	}
	return true
}

func isNodeReady(node *corev1.Node) bool {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == corev1.NodeReady &&
			node.Status.Conditions[i].Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func getNodeInternalIPs(node *corev1.Node) []string {
	var nodeIPs []string
	for _, nodeAddr := range node.Status.Addresses {
		if nodeAddr.Type == corev1.NodeInternalIP {
			nodeIPs = append(nodeIPs, nodeAddr.Address)
			break
		}
	}
	return nodeIPs
}

// ensurePortsIptables ensures jump chains and rules for active dnat ports, and
// delete the jump chains if their corresponding dnat ports are removed
func (im *iptablesManager) ensurePortsIptables(currentPorts, deletedPorts, currentIPs, deletedIPs []string) error {
	// for each dnat port, we create a jump chain
	jumpChains := iptablesJumpChains
	for _, port := range currentPorts {
		jumpChains = append(jumpChains, iptablesJumpChain{
			table:     iptables.TableNAT,
			dstChain:  iptables.Chain(fmt.Sprintf("%s%s", yurttunnelPortChainPrefix, port)),
			srcChain:  yurttunnelServerPortChain,
			comment:   fmt.Sprintf("jump to port %s", port),
			extraArgs: []string{"-p", "tcp", "--dport", port},
		})
	}
	if err := im.ensureJumpChains(jumpChains); err != nil {
		klog.Errorf("Failed to ensure jump chain, %v", err)
		return err
	}

	// ensure iptable rule for each dnat port
	for _, port := range currentPorts {
		err := im.ensurePortIptables(port, currentIPs, deletedIPs)
		if err != nil {
			return err
		}
	}

	if len(deletedPorts) == 0 {
		return nil
	}

	// if certain dnat ports are removed, delete the corresponding chains
	var deletedJumpChains []iptablesJumpChain
	for _, port := range deletedPorts {
		deletedJumpChains = append(deletedJumpChains, iptablesJumpChain{
			table:     iptables.TableNAT,
			dstChain:  iptables.Chain(fmt.Sprintf("%s%s", yurttunnelPortChainPrefix, port)),
			srcChain:  yurttunnelServerPortChain,
			comment:   fmt.Sprintf("jump to port %s", port),
			extraArgs: []string{"-p", "tcp", "--dport", port},
		})
	}
	if err := im.deleteJumpChains(deletedJumpChains); err != nil {
		klog.Errorf("Failed to delete jump chain, %v", err)
		return err
	}

	return nil
}

func (im *iptablesManager) ensurePortIptables(port string, currentIPs, deletedIPs []string) error {
	portChain := iptables.Chain(fmt.Sprintf("%s%s", yurttunnelPortChainPrefix, port))

	if len(currentIPs) == 0 {
		_ = im.iptables.FlushChain(iptables.TableNAT, portChain)
		return nil
	}

	// ensure chains for dnat ports
	if _, err := im.iptables.EnsureChain(iptables.TableNAT, portChain); err != nil {
		klog.Errorf("could not ensure chain for tunnel server port(%s), %v", port, err)
		return err
	}

	// decide the proxy destination based on the port number
	proxyDest := im.insecureDnatDest
	if port == kubeletSecurePort {
		proxyDest = im.secureDnatDest
	}

	// do not proxy packets, whose destination node doesn't has agent running
	for _, ip := range currentIPs {
		reqReturnPortIptablesArgs := reqReturnIptablesArgs(reqReturnComment, port, ip)
		_, err := im.iptables.EnsureRule(
			iptables.Prepend,
			iptables.TableNAT, portChain, reqReturnPortIptablesArgs...)
		if err != nil {
			klog.Errorf("could not ensure -j RETURN iptables rule for %s:%s: %v", ip, port, err)
			return err
		}
	}

	// for the rest of the packets, redirect them to the proxy server, i.e., yurttunnel-server
	dnatPortIptablesArgs := dnatIptablesArgs(dnatToTunnelComment, port, proxyDest)
	_, err := im.iptables.EnsureRule(
		iptables.Append,
		iptables.TableNAT, portChain, dnatPortIptablesArgs...)
	if err != nil {
		klog.Errorf("could not ensure dnat iptables rule for %s, %v", port, err)
		return err
	}

	// delete iptable rules related to nodes that have been deleted
	for _, ip := range deletedIPs {
		deletedIPIptablesArgs := reqReturnIptablesArgs(reqReturnComment, port, ip)
		err = im.iptables.DeleteRule(iptables.TableNAT,
			portChain, deletedIPIptablesArgs...)
		if err != nil {
			klog.Errorf("could not delete old iptables rules for %s:%s: %v", ip, port, err)
			return err
		}
	}

	return nil
}

func (im *iptablesManager) ensureJumpChains(jumpChains []iptablesJumpChain) error {
	for _, jump := range jumpChains {
		if _, err := im.iptables.EnsureChain(jump.table, jump.dstChain); err != nil {
			klog.Errorf("could not ensure that %s chain %s exists: %v",
				jump.table, jump.dstChain, err)
			return err
		}
		args := append(jump.extraArgs,
			"-m", "comment", "--comment", jump.comment,
			"-j", string(jump.dstChain))

		if _, err := im.iptables.EnsureRule(
			iptables.Prepend,
			jump.table,
			jump.srcChain, args...); err != nil {
			klog.Errorf("failed to ensure that %s chain %s jumps to %s: %v",
				jump.table, jump.srcChain, jump.dstChain, err)
			return err
		}
	}
	return nil
}

func dnatIptablesArgs(msg, destPort, proxyDest string) []string {
	args := iptablesCommonArgs(msg, destPort, nil)
	args = append(args, "-j", "DNAT", "--to-destination", proxyDest)
	return args
}

func reqReturnIptablesArgs(msg, destPort, ip string) []string {
	destIP := net.ParseIP(ip)
	args := iptablesCommonArgs(msg, destPort, destIP)
	args = append(args, "-j", "RETURN")
	return args
}

func iptablesCommonArgs(msg, destPort string, destIP net.IP) []string {
	args := []string{
		"-p", "tcp",
		"-m", "comment",
	}
	if len(msg) != 0 {
		args = append(args, "--comment", msg)
	}
	if len(destPort) != 0 {
		args = append(args, "--dport", destPort)
	}
	if destIP != nil {
		ip := toCIDR(destIP)
		args = append(args, "-d", ip)
	}
	return args
}

func toCIDR(ip net.IP) string {
	size := 32
	// if not an IPv4 address, set the number of bits as 128
	if ip.To4() == nil {
		size = 128
	}
	return fmt.Sprintf("%s/%d", ip.String(), size)
}

func (im *iptablesManager) clearConnTrackEntries(ips, ports []string) {
	if len(im.conntrackPath) == 0 {
		return
	}
	klog.Infof("clear conntrack entries for ports %q and nodes %q", ports, ips)
	for _, port := range ports {
		for _, ip := range ips {
			im.clearConnTrackEntriesForIPPort(ip, port)
		}
	}
}

func (im *iptablesManager) clearConnTrackEntriesForIPPort(ip, port string) {
	parameters := parametersWithFamily(utilnet.IsIPv6String(ip),
		"-D", "--orig-dst",
		ip, "-p",
		"tcp", "--dport", port)
	output, err := im.execer.
		Command(im.conntrackPath, parameters...).
		CombinedOutput()
	if err != nil {
		klog.Errorf("clear conntrack for %s:%s failed: %q, error message: %s",
			ip, port, string(output), err)
		return
	}
	klog.Infof("clear conntrack for %s:%s successfully: %q",
		ip, port, string(output))
}

func parametersWithFamily(isIPv6 bool, parameters ...string) []string {
	if isIPv6 {
		parameters = append(parameters, "-f", "ipv6")
	}
	return parameters
}

// syncIptableSetting update all of iptables chains and rules.
// the request to access the edge node is forwarded to the tunnel server
// while the request to access the cloud node is returned
func (im *iptablesManager) syncIptableSetting() {
	// check if there are new dnat ports
	dnatPorts, err := util.GetConfiguredDnatPorts(im.kubeClient, im.insecurePort)
	if err != nil {
		klog.Errorf("failed to sync iptables rules, %v", err)
		return
	}
	portsChanged, deletedDnatPorts := im.getDeletedPorts(dnatPorts)
	currentDnatPorts := append(dnatPorts, kubeletSecurePort, kubeletInsecurePort)

	// check if there are new nodes
	nodesIP := im.getIPOfNodesWithoutAgent()
	nodesChanged, addedNodesIP, deletedNodesIP := im.getAddedAndDeletedNodes(nodesIP)
	currentNodesIP := append(nodesIP, loopbackAddr)

	// update the iptable setting if necessary
	err = im.ensurePortsIptables(currentDnatPorts, deletedDnatPorts, currentNodesIP, deletedNodesIP)
	if err != nil {
		klog.Errorf("failed to ensurePortsIptables: %v", err)
		return
	}

	if portsChanged {
		im.lastDnatPorts = dnatPorts
		// we don't need to clear conntrack entries for newly added dnat ports,
		if len(deletedDnatPorts) != 0 {
			im.clearConnTrackEntries(currentNodesIP, deletedDnatPorts)
		}
		klog.Infof("dnat ports changed, %v", dnatPorts)
	}

	if nodesChanged {
		im.lastNodesIP = nodesIP
		im.clearConnTrackEntries(append(addedNodesIP, deletedNodesIP...), currentDnatPorts)
		klog.Infof("directly access nodes changed, %v for ports %v", nodesIP, currentDnatPorts)
	}
}

func (im *iptablesManager) getAddedAndDeletedNodes(currentNodesIP []string) (bool, []string, []string) {
	changed := false
	if len(im.lastNodesIP) != len(currentNodesIP) {
		changed = true
	}
	addedNodesIP := make([]string, 0)
	for i := range currentNodesIP {
		found := false
		for j := range im.lastNodesIP {
			if currentNodesIP[i] == im.lastNodesIP[j] {
				found = true
				break
			}
		}

		if !found {
			addedNodesIP = append(addedNodesIP, currentNodesIP[i])
			changed = true
		}
	}

	deletedNodesIP := make([]string, 0)
	for i := range im.lastNodesIP {
		found := false
		for j := range currentNodesIP {
			if im.lastNodesIP[i] == currentNodesIP[j] {
				found = true
				break
			}
		}

		if !found {
			deletedNodesIP = append(deletedNodesIP, im.lastNodesIP[i])
			changed = true
		}
	}

	return changed, addedNodesIP, deletedNodesIP
}

func (im *iptablesManager) getDeletedPorts(currentPorts []string) (bool, []string) {
	changed := false
	if len(im.lastDnatPorts) != len(currentPorts) {
		changed = true
	}
	var deletedPorts []string
	for i := range im.lastDnatPorts {
		found := false
		for j := range currentPorts {
			if im.lastDnatPorts[i] == currentPorts[j] {
				found = true
				break
			}
		}

		if !found {
			deletedPorts = append(deletedPorts, im.lastDnatPorts[i])
			changed = true
		}
	}
	return changed, deletedPorts
}
