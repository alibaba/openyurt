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
package serveraddr

import (
	"fmt"
	"net"
	"reflect"
	"testing"

	"github.com/openyurtio/openyurt/pkg/yurttunnel/constants"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetDefaultDomainsForSvcInputParamEmptyChar(t *testing.T) {
	domains := GetDefaultDomainsForSvc("", "")
	if len(domains) != 0 {
		t.Error("domains len is not equal zero")
	}
}

func TestGetDefaultDomainsForSvc(t *testing.T) {
	ns := "hello"
	name := "world"
	domains := GetDefaultDomainsForSvc(ns, name)
	if len(domains) == 0 {
		t.Log("domains len is zero")
	} else {
		if len(domains) == 4 {
			if name != domains[0] {
				t.Errorf("The two words should be the same:%s\n", name)
			}
			if fmt.Sprintf("%s.%s", name, ns) != domains[1] {
				t.Errorf("The two words should be the same,%s.%s\n", name, ns)
			}
			if fmt.Sprintf("%s.%s.svc", name, ns) != domains[2] {
				t.Errorf("The two words should be the same,%s.%s.svc", name, ns)
			}
			if fmt.Sprintf("%s.%s.svc.cluster.local", name, ns) != domains[3] {
				t.Errorf("The two words should be the same,%s.%s.svc.cluster.local", name, ns)
			}
		}
	}
}

func TestGetNodePortDNSandIP(t *testing.T) {
	type ExpectValue struct {
		ErrorMsg string
		dnsNames []string
		ips      []net.IP
	}

	tests := []struct {
		desc        string
		nodelist    v1.NodeList
		expectValue ExpectValue
	}{
		{
			desc:     "there is no cloud node",
			nodelist: v1.NodeList{},
			expectValue: ExpectValue{
				ErrorMsg: "there is no cloud node",
			},
		},
		{
			desc: "there is no cloud node",
			nodelist: v1.NodeList{
				Items: []v1.Node{
					{
						Status: v1.NodeStatus{
							Addresses: []v1.NodeAddress{
								{
									Type:    v1.NodeInternalDNS,
									Address: "192.168.1.1",
								},
							},
						},
					},
				},
			},
			expectValue: ExpectValue{
				ErrorMsg: "can't find node IP",
			},
		},
		{
			desc: "Get IPs Error",
			nodelist: v1.NodeList{
				Items: []v1.Node{
					{
						Status: v1.NodeStatus{
							Addresses: []v1.NodeAddress{
								{
									Type:    v1.NodeInternalIP,
									Address: "192.168.1.1",
								},
							},
						},
					},
				},
			},
			expectValue: ExpectValue{
				ErrorMsg: "Get IPs Error",
				ips: []net.IP{
					net.ParseIP("192.168.1.1"),
				},
			},
		},
		{
			desc: "Many IPs",
			nodelist: v1.NodeList{
				Items: []v1.Node{
					{
						Status: v1.NodeStatus{
							Addresses: []v1.NodeAddress{
								{
									Type:    v1.NodeInternalIP,
									Address: "192.168.1.1",
								},
								{
									Type:    v1.NodeHostName,
									Address: "192.168.1.2",
								},
								{
									Type:    v1.NodeExternalDNS,
									Address: "192.168.1.3",
								},
								{
									Type:    v1.NodeInternalIP,
									Address: "192.168.1.4",
								},
							},
						},
					},
				},
			},
			expectValue: ExpectValue{
				ErrorMsg: "IPs is Invalid.",
				ips: []net.IP{
					net.ParseIP("192.168.1.1"),
					net.ParseIP("192.168.1.4"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			//getNodePortDNSandIP(nodeLst *v1.NodeList) ([]string, []net.IP, error)
			_, getIPS, error := getNodePortDNSandIP(&tt.nodelist)
			if error != nil {
				if error.Error() != tt.expectValue.ErrorMsg {
					t.Errorf("error is %s\n", tt.expectValue.ErrorMsg)
				}
			} else {
				if !reflect.DeepEqual(getIPS, tt.expectValue.ips) {
					t.Error(tt.expectValue.ErrorMsg)
				}
			}
		})
	}

}

func TestGetClusterIPDNSandIPForIP(t *testing.T) {
	type ExpectValue struct {
		ErrorMsg string
		dnsNames []string
		ips      []net.IP
	}

	tests := []struct {
		desc        string
		svc         corev1.Service
		expectValue ExpectValue
	}{
		{
			desc: "there is constants.YurttunnelServerExternalAddrKey ips is  192.168.1.2",
			svc: corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.YurttunnelServerExternalAddrKey: "192.168.1.2:8080",
					},
				},
			},
			expectValue: ExpectValue{
				ErrorMsg: "there is constants.YurttunnelServerExternalAddrKey ips is  192.168.1.2",
				ips: []net.IP{
					net.ParseIP("192.168.1.2"),
				},
				dnsNames: []string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			dnsNames, getIPS, err := getClusterIPDNSandIP(&tt.svc)
			if err != nil {
				t.Errorf("error is %v\n", err)
			} else {
				if !reflect.DeepEqual(dnsNames, tt.expectValue.dnsNames) ||
					!reflect.DeepEqual(getIPS, tt.expectValue.ips) {
					t.Error(tt.expectValue.ErrorMsg)
				}
			}
		})
	}

}
func TestGetClusterIPDNSandIPForIPDNS(t *testing.T) {
	type ExpectValue struct {
		ErrorMsg string
		dnsNames []string
		ips      []net.IP
	}

	tests := []struct {
		desc        string
		svc         corev1.Service
		expectValue ExpectValue
	}{
		{
			desc: "there is constants.YurttunnelServerExternalAddrKey dnsnames is  www.bing.com:80",
			svc: corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						constants.YurttunnelServerExternalAddrKey: "www.bing.com:80",
					},
				},
			},
			expectValue: ExpectValue{
				ErrorMsg: "there is constants.YurttunnelServerExternalAddrKey dnsnames is  www.bing.com:80",
				dnsNames: []string{
					"www.bing.com",
				},
				ips: []net.IP{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			dnsNames, getIPS, err := getClusterIPDNSandIP(&tt.svc)
			if err != nil {
				t.Errorf("error is %v\n", err)
			} else {
				if !reflect.DeepEqual(dnsNames, tt.expectValue.dnsNames) ||
					!reflect.DeepEqual(getIPS, tt.expectValue.ips) {
					t.Error(tt.expectValue.ErrorMsg)
				}
			}
		})
	}

}
