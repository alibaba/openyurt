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

package remote

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/openyurtio/openyurt/pkg/yurthub/cachemanager"
	"github.com/openyurtio/openyurt/pkg/yurthub/certificate/interfaces"
	"github.com/openyurtio/openyurt/pkg/yurthub/healthchecker"
	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/meta"
	util2 "github.com/openyurtio/openyurt/pkg/yurthub/proxy/util"
	"github.com/openyurtio/openyurt/pkg/yurthub/transport"
	"github.com/openyurtio/openyurt/pkg/yurthub/util"

	"k8s.io/klog"
)

type loadBalancerAlgo interface {
	PickOne() *RemoteProxy
	Name() string
}

type rrLoadBalancerAlgo struct {
	sync.Mutex
	backends []*RemoteProxy
	next     int
}

func (rr *rrLoadBalancerAlgo) Name() string {
	return "rr algorithm"
}

func (rr *rrLoadBalancerAlgo) PickOne() *RemoteProxy {
	if len(rr.backends) == 0 {
		return nil
	} else if len(rr.backends) == 1 {
		if rr.backends[0].IsHealthy() {
			return rr.backends[0]
		}
		return nil
	} else {
		// round robin
		rr.Lock()
		defer rr.Unlock()
		hasFound := false
		selected := rr.next
		for i := 0; i < len(rr.backends); i++ {
			selected = (rr.next + i) % len(rr.backends)
			if rr.backends[selected].IsHealthy() {
				hasFound = true
				break
			}
		}

		if hasFound {
			rr.next = (selected + 1) % len(rr.backends)
			return rr.backends[selected]
		}
	}

	return nil
}

type priorityLoadBalancerAlgo struct {
	sync.Mutex
	backends []*RemoteProxy
}

func (prio *priorityLoadBalancerAlgo) Name() string {
	return "priority algorithm"
}

func (prio *priorityLoadBalancerAlgo) PickOne() *RemoteProxy {
	if len(prio.backends) == 0 {
		return nil
	} else if len(prio.backends) == 1 {
		if prio.backends[0].IsHealthy() {
			return prio.backends[0]
		}
		return nil
	} else {
		prio.Lock()
		defer prio.Unlock()
		for i := 0; i < len(prio.backends); i++ {
			if prio.backends[i].IsHealthy() {
				return prio.backends[i]
			}
		}

		return nil
	}
}

// LoadBalancer is an interface for proxying http request to remote server
// based on the load balance mode(round-robin or priority)
type LoadBalancer interface {
	IsHealthy() bool
	ServeHTTP(rw http.ResponseWriter, req *http.Request)
}

type loadBalancer struct {
	backends          []*RemoteProxy
	algo              loadBalancerAlgo
	certManager       interfaces.YurtCertificateManager
	restMapperManager *meta.RESTMapperManager
}

// NewLoadBalancer creates a loadbalancer for specified remote servers
func NewLoadBalancer(
	lbMode string,
	remoteServers []*url.URL,
	cacheMgr cachemanager.CacheManager,
	transportMgr transport.Interface,
	healthChecker healthchecker.HealthChecker,
	certManager interfaces.YurtCertificateManager,
	restMapperManager *meta.RESTMapperManager,
	stopCh <-chan struct{}) (LoadBalancer, error) {
	backends := make([]*RemoteProxy, 0, len(remoteServers))
	for i := range remoteServers {
		b, err := NewRemoteProxy(remoteServers[i], cacheMgr, transportMgr, healthChecker, stopCh)
		if err != nil {
			klog.Errorf("could not new proxy backend(%s), %v", remoteServers[i].String(), err)
			continue
		}
		backends = append(backends, b)
	}
	if len(backends) == 0 {
		return nil, fmt.Errorf("no backends can be used by lb")
	}

	var algo loadBalancerAlgo
	switch lbMode {
	case "rr":
		algo = &rrLoadBalancerAlgo{backends: backends}
	case "priority":
		algo = &priorityLoadBalancerAlgo{backends: backends}
	default:
		algo = &rrLoadBalancerAlgo{backends: backends}
	}

	return &loadBalancer{
		backends:          backends,
		algo:              algo,
		certManager:       certManager,
		restMapperManager: restMapperManager,
	}, nil
}

func (lb *loadBalancer) IsHealthy() bool {
	// both certificate is not expired and
	// have at least one healthy remote server,
	// load balancer can proxy the request to
	// remote server
	if lb.certManager.NotExpired() {
		for i := range lb.backends {
			if lb.backends[i].IsHealthy() {
				return true
			}
		}
	}
	return false
}

func (lb *loadBalancer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	b := lb.algo.PickOne()
	if b == nil {
		// exceptional case
		klog.Errorf("could not pick one healthy backends by %s for request %s", lb.algo.Name(), util.ReqString(req))
		http.Error(rw, "could not pick one healthy backends, try again to go through local proxy.", http.StatusInternalServerError)
		return
	}
	klog.V(3).Infof("picked backend %s by %s for request %s", b.Name(), lb.algo.Name(), util.ReqString(req))
	handler := util2.WithUpdateRESTMapper(b, lb.restMapperManager)
	handler.ServeHTTP(rw, req)
}
