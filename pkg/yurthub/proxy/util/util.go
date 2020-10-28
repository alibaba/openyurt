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

package util

import (
	"net/http"
	"strings"
	"time"

	"github.com/alibaba/openyurt/pkg/yurthub/util"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog"
)

const (
	canCacheHeader string = "Edge-Cache"
)

// WithRequestContentType add req-content-type in request context.
// if no Accept header is set, application/vnd.kubernetes.protobuf will be used
func WithRequestContentType(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if info, ok := apirequest.RequestInfoFrom(ctx); ok {
			if info.IsResourceRequest {
				var contentType string
				header := req.Header.Get("Accept")
				parts := strings.Split(header, ",")
				if len(parts) >= 1 {
					contentType = parts[0]
				}

				if len(contentType) == 0 {
					klog.Errorf("no accept content type for request: %s", util.ReqString(req))
					http.Error(w, "no accept content type is set.", http.StatusBadRequest)
					return
				}

				ctx = util.WithReqContentType(ctx, contentType)
				req = req.WithContext(ctx)
			}
		}

		handler.ServeHTTP(w, req)
	})
}

// WithCacheHeaderCheck add cache agent for response cache
// in default mode, only kubelet, kube-proxy, flanneld, coredns User-Agent
// can be supported to cache response. and with Edge-Cache header is also supported.
func WithCacheHeaderCheck(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if info, ok := apirequest.RequestInfoFrom(ctx); ok {
			if info.IsResourceRequest {
				needToCache := strings.ToLower(req.Header.Get(canCacheHeader))
				if needToCache == "true" {
					ctx = util.WithReqCanCache(ctx, true)
					req = req.WithContext(ctx)
				}
				req.Header.Del(canCacheHeader)
			}
		}

		handler.ServeHTTP(w, req)
	})
}

// WithRequestClientComponent add component field in request context.
// component is extracted from User-Agent Header, and only the content
// before the "/" when User-Agent include "/".
func WithRequestClientComponent(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if info, ok := apirequest.RequestInfoFrom(ctx); ok {
			if info.IsResourceRequest {
				var comp string
				userAgent := strings.ToLower(req.Header.Get("User-Agent"))
				parts := strings.Split(userAgent, "/")
				if len(parts) > 0 {
					comp = strings.ToLower(parts[0])
				}

				if comp != "" {
					ctx = util.WithClientComponent(ctx, comp)
					req = req.WithContext(ctx)
				}
			}
		}

		handler.ServeHTTP(w, req)
	})
}

type wrapperResponseWriter struct {
	http.ResponseWriter
	http.Flusher
	http.CloseNotifier
	statusCode int
}

func newWrapperResponseWriter(w http.ResponseWriter) *wrapperResponseWriter {
	cn, ok := w.(http.CloseNotifier)
	if !ok {
		klog.Error("can not get http.CloseNotifier")
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		klog.Error("can not get http.Flusher")
	}

	return &wrapperResponseWriter{
		ResponseWriter: w,
		Flusher:        flusher,
		CloseNotifier:  cn,
	}
}

func (wrw *wrapperResponseWriter) WriteHeader(statusCode int) {
	wrw.statusCode = statusCode
	wrw.ResponseWriter.WriteHeader(statusCode)
}

// WithRequestTrace used to trace status code and handle time for request.
func WithRequestTrace(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		wrapperRW := newWrapperResponseWriter(w)
		start := time.Now()
		handler.ServeHTTP(wrapperRW, req)
		klog.Infof("%s with status code %d, spent %v", util.ReqString(req), wrapperRW.statusCode, time.Since(start))
	})
}

// WithMaxInFlightLimit limits the number of in-flight requests. and when in flight
// requests exceeds the threshold, the following incoming requests will be rejected.
func WithMaxInFlightLimit(handler http.Handler, limit int) http.Handler {
	var reqChan chan bool
	if limit > 0 {
		reqChan = make(chan bool, limit)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		select {
		case reqChan <- true:
			handler.ServeHTTP(w, req)
			<-reqChan
			klog.Infof("%s request completed, left %d requests in flight", util.ReqString(req), len(reqChan))
		default:
			// Return a 429 status indicating "Too Many Requests"
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Too many requests, please try again later.", http.StatusTooManyRequests)
		}
	})
}
