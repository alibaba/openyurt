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

package proxy

import (
	"net/http"

	"github.com/openyurtio/openyurt/cmd/yurthub/app/config"
	"github.com/openyurtio/openyurt/pkg/yurthub/cachemanager"
	"github.com/openyurtio/openyurt/pkg/yurthub/certificate/interfaces"
	"github.com/openyurtio/openyurt/pkg/yurthub/healthchecker"
	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/meta"
	"github.com/openyurtio/openyurt/pkg/yurthub/proxy/local"
	"github.com/openyurtio/openyurt/pkg/yurthub/proxy/remote"
	"github.com/openyurtio/openyurt/pkg/yurthub/proxy/util"
	"github.com/openyurtio/openyurt/pkg/yurthub/transport"
	hubutil "github.com/openyurtio/openyurt/pkg/yurthub/util"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server"
)

type yurtReverseProxy struct {
	resolver            apirequest.RequestInfoResolver
	loadBalancer        remote.LoadBalancer
	localProxy          *local.LocalProxy
	cacheMgr            cachemanager.CacheManager
	maxRequestsInFlight int
	stopCh              <-chan struct{}
}

// NewYurtReverseProxyHandler creates a http handler for proxying
// all of incoming requests.
func NewYurtReverseProxyHandler(
	yurtHubCfg *config.YurtHubConfiguration,
	cacheMgr cachemanager.CacheManager,
	transportMgr transport.Interface,
	healthChecker healthchecker.HealthChecker,
	certManager interfaces.YurtCertificateManager,
	restMapperManager *meta.RESTMapperManager,
	stopCh <-chan struct{}) (http.Handler, error) {
	cfg := &server.Config{
		LegacyAPIGroupPrefixes: sets.NewString(server.DefaultLegacyAPIPrefix),
	}
	resolver := server.NewRequestInfoResolver(cfg)

	lb, err := remote.NewLoadBalancer(
		yurtHubCfg.LBMode,
		yurtHubCfg.RemoteServers,
		cacheMgr,
		transportMgr,
		healthChecker,
		certManager,
		restMapperManager,
		stopCh)
	if err != nil {
		return nil, err
	}

	yurtProxy := &yurtReverseProxy{
		resolver:            resolver,
		loadBalancer:        lb,
		localProxy:          local.NewLocalProxy(cacheMgr, lb.IsHealthy),
		cacheMgr:            cacheMgr,
		maxRequestsInFlight: yurtHubCfg.MaxRequestInFlight,
		stopCh:              stopCh,
	}

	return yurtProxy.buildHandlerChain(yurtProxy), nil
}

func (p *yurtReverseProxy) buildHandlerChain(handler http.Handler) http.Handler {
	handler = util.WithRequestTrace(handler)
	handler = util.WithRequestContentType(handler)
	handler = util.WithCacheHeaderCheck(handler)
	handler = util.WithRequestTimeout(handler)
	handler = util.WithListRequestSelector(handler)
	handler = util.WithRequestClientComponent(handler)
	handler = filters.WithRequestInfo(handler, p.resolver)
	handler = util.WithMaxInFlightLimit(handler, p.maxRequestsInFlight)
	return handler
}

func (p *yurtReverseProxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if !hubutil.IsKubeletLeaseReq(req) && p.loadBalancer.IsHealthy() {
		p.loadBalancer.ServeHTTP(rw, req)
	} else {
		p.localProxy.ServeHTTP(rw, req)
	}
}
