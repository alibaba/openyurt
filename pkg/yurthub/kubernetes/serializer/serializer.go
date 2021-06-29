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

package serializer

import (
	"bytes"
	"fmt"
	"io"
	"mime"
	"strings"
	"sync"

	"github.com/openyurtio/openyurt/pkg/yurthub/kubernetes/meta"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured/unstructuredscheme"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	restclientwatch "k8s.io/client-go/rest/watch"
	"k8s.io/klog"
)

// YurtHubSerializer is a global serializer manager for yurthub
var YurtHubSerializer = NewSerializerManager()

type yurtClientNegotiator struct {
	recognized bool
	runtime.ClientNegotiator
}

// SerializerManager is responsible for managing *rest.Serializers
type SerializerManager struct {
	sync.Mutex
	// NegotiatedSerializer is used for obtaining encoders and decoders for multiple
	// supported media types.
	NegotiatedSerializer runtime.NegotiatedSerializer
	// UnstructuredNegotiatedSerializer is used to obtain encoders and decoders
	// for resources not registered in the scheme
	UnstructuredNegotiatedSerializer runtime.NegotiatedSerializer
	// ClientNegotiators includes all of ClientNegotiators by GroupVersionResource
	ClientNegotiators map[schema.GroupVersionResource]*yurtClientNegotiator
	// WatchEventClientNegotiator is a ClientNegotiators for WatchEvent
	WatchEventClientNegotiator runtime.ClientNegotiator
}

// NewSerializerManager creates a *SerializerManager object with no version conversion
func NewSerializerManager() *SerializerManager {
	sm := &SerializerManager{
		// do not need version conversion, and keep the gvk information
		NegotiatedSerializer:             WithVersionCodecFactory{CodecFactory: scheme.Codecs},
		UnstructuredNegotiatedSerializer: NewUnstructuredNegotiatedSerializer(),
		ClientNegotiators:                make(map[schema.GroupVersionResource]*yurtClientNegotiator),
	}

	watchEventGVR := metav1.SchemeGroupVersion.WithResource("watchevents")
	sm.WatchEventClientNegotiator = runtime.NewClientNegotiator(sm.NegotiatedSerializer, watchEventGVR.GroupVersion())
	sm.ClientNegotiators[watchEventGVR] = &yurtClientNegotiator{recognized: true, ClientNegotiator: sm.WatchEventClientNegotiator}
	return sm
}

// GetNegotiatedSerializer returns an NegotiatedSerializer object based on GroupVersionResource
func (sm *SerializerManager) GetNegotiatedSerializer(gvr schema.GroupVersionResource) runtime.NegotiatedSerializer {
	if isScheme := meta.IsSchemeResource(gvr); isScheme {
		return sm.NegotiatedSerializer
	}
	return sm.UnstructuredNegotiatedSerializer
}

// WithVersionCodecFactory is a CodecFactory that will explicitly ignore requests to perform conversion.
// It keeps the gvk during deserialization.
// This wrapper is used while code migrates away from using conversion (such as external clients)
type WithVersionCodecFactory struct {
	serializer.CodecFactory
}

// EncoderForVersion returns an encoder that does not do conversion, but does set the group version kind of the object
// when serialized.
func (f WithVersionCodecFactory) EncoderForVersion(serializer runtime.Encoder, version runtime.GroupVersioner) runtime.Encoder {
	return runtime.WithVersionEncoder{
		Version:     version,
		Encoder:     serializer,
		ObjectTyper: scheme.Scheme,
	}
}

// DecoderToVersion returns an decoder that does not do conversion, and keeps the gvk information
func (f WithVersionCodecFactory) DecoderToVersion(serializer runtime.Decoder, _ runtime.GroupVersioner) runtime.Decoder {
	return WithVersionDecoder{
		Decoder: serializer,
	}
}

// WithVersionDecoder keeps the group version kind of a deserialized object.
type WithVersionDecoder struct {
	runtime.Decoder
}

// Decode does not do conversion. It keeps the gvk during deserialization.
func (d WithVersionDecoder) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	return d.Decoder.Decode(data, defaults, into)
}

type UnstructuredNegotiatedSerializer struct {
	scheme  *runtime.Scheme
	typer   runtime.ObjectTyper
	creator runtime.ObjectCreater
}

// NewUnstructuredNegotiatedSerializer returns a negotiated serializer for Unstructured resources
func NewUnstructuredNegotiatedSerializer() runtime.NegotiatedSerializer {
	return UnstructuredNegotiatedSerializer{
		scheme:  scheme.Scheme,
		typer:   unstructuredscheme.NewUnstructuredObjectTyper(),
		creator: NewUnstructuredCreator(),
	}
}

func (s UnstructuredNegotiatedSerializer) SupportedMediaTypes() []runtime.SerializerInfo {
	return []runtime.SerializerInfo{
		{
			MediaType:        "application/json",
			MediaTypeType:    "application",
			MediaTypeSubType: "json",
			EncodesAsText:    true,
			Serializer:       json.NewSerializerWithOptions(json.DefaultMetaFactory, s.creator, s.typer, json.SerializerOptions{}),
			PrettySerializer: json.NewSerializerWithOptions(json.DefaultMetaFactory, s.creator, s.typer, json.SerializerOptions{Pretty: true}),
			StreamSerializer: &runtime.StreamSerializerInfo{
				EncodesAsText: true,
				Serializer:    json.NewSerializerWithOptions(json.DefaultMetaFactory, s.creator, s.typer, json.SerializerOptions{}),
				Framer:        json.Framer,
			},
		},
		{
			MediaType:        "application/yaml",
			MediaTypeType:    "application",
			MediaTypeSubType: "yaml",
			EncodesAsText:    true,
			Serializer:       json.NewSerializerWithOptions(json.DefaultMetaFactory, s.creator, s.typer, json.SerializerOptions{Yaml: true}),
		},
	}
}

//EncoderForVersion do nothing, but returns a encoder,
//if the object is unstructured, the encoder will encode object without conversion
func (s UnstructuredNegotiatedSerializer) EncoderForVersion(encoder runtime.Encoder, _ runtime.GroupVersioner) runtime.Encoder {
	return encoder
}

//DecoderToVersion do nothing, and returns a decoder that does not do conversion
func (s UnstructuredNegotiatedSerializer) DecoderToVersion(decoder runtime.Decoder, _ runtime.GroupVersioner) runtime.Decoder {
	return WithVersionDecoder{
		Decoder: decoder,
	}
}

type unstructuredCreator struct{}

// NewUnstructuredCreator returns a simple object creator that always returns an Unstructured or UnstructuredList
func NewUnstructuredCreator() runtime.ObjectCreater {
	return unstructuredCreator{}
}

func (c unstructuredCreator) New(kind schema.GroupVersionKind) (runtime.Object, error) {
	if strings.HasSuffix(kind.Kind, "List") {
		ret := &unstructured.UnstructuredList{}
		ret.SetGroupVersionKind(kind)
		return ret, nil
	} else {
		ret := &unstructured.Unstructured{}
		ret.SetGroupVersionKind(kind)
		return ret, nil
	}
}

// genClientNegotiator creates a ClientNegotiator for specified GroupVersionResource and gvr is recognized or not
func (sm *SerializerManager) genClientNegotiator(gvr schema.GroupVersionResource) (runtime.ClientNegotiator, bool) {
	if isScheme := meta.IsSchemeResource(gvr); isScheme {
		return runtime.NewClientNegotiator(sm.NegotiatedSerializer, gvr.GroupVersion()), true
	}
	klog.Infof("%#+v is not found in client-go runtime scheme", gvr)
	return runtime.NewClientNegotiator(sm.UnstructuredNegotiatedSerializer, gvr.GroupVersion()), false
}

// Serializer is used for transforming objects into a serialized format and back for cache manager of hub agent.
type Serializer struct {
	recognized  bool
	contentType string
	runtime.ClientNegotiator
	watchEventClientNegotiator runtime.ClientNegotiator
}

// CreateSerializer will returns a Serializer object for encoding or decoding runtime object.
func (sm *SerializerManager) CreateSerializer(contentType, group, version, resource string) *Serializer {
	var recognized bool
	var clientNegotiator runtime.ClientNegotiator
	gvr := schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: resource,
	}

	if len(contentType) == 0 {
		return nil
	}

	sm.Lock()
	defer sm.Unlock()
	if cn, ok := sm.ClientNegotiators[gvr]; ok {
		clientNegotiator, recognized = cn.ClientNegotiator, cn.recognized
	} else {
		clientNegotiator, recognized = sm.genClientNegotiator(gvr)
		sm.ClientNegotiators[gvr] = &yurtClientNegotiator{
			recognized:       recognized,
			ClientNegotiator: clientNegotiator,
		}
	}

	return &Serializer{
		recognized:                 recognized,
		contentType:                contentType,
		ClientNegotiator:           clientNegotiator,
		watchEventClientNegotiator: sm.WatchEventClientNegotiator,
	}
}

// Decode decodes byte data into runtime object with embedded contentType.
func (s *Serializer) Decode(b []byte) (runtime.Object, error) {
	var decoder runtime.Decoder
	if len(b) == 0 {
		return nil, fmt.Errorf("0-length response body, content type: %s", s.contentType)
	}

	mediaType, params, err := mime.ParseMediaType(s.contentType)
	if err != nil {
		return nil, err
	}

	decoder, err = s.Decoder(mediaType, params)
	if err != nil {
		return nil, err
	}

	out, _, err := decoder.Decode(b, nil, nil)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WatchDecoder generates a Decoder for decoding response of watch request.
func (s *Serializer) WatchDecoder(body io.ReadCloser) (*restclientwatch.Decoder, error) {
	var err error
	var embeddedObjectDecoder runtime.Decoder
	var streamingSerializer runtime.Serializer
	var framer runtime.Framer
	mediaType, params, err := mime.ParseMediaType(s.contentType)
	if err != nil {
		return nil, err
	}

	embeddedObjectDecoder, streamingSerializer, framer, err = s.StreamDecoder(mediaType, params)
	if err != nil {
		return nil, err
	}
	if !s.recognized {
		// if gvr is not recognized, maybe it's a crd resource, so do not use the same streaming
		// serializer to decode watch event object, and instead use watch event client negotiator.
		_, streamingSerializer, framer, err = s.watchEventClientNegotiator.StreamDecoder(mediaType, params)
		if err != nil {
			return nil, err
		}
	}

	frameReader := framer.NewFrameReader(body)
	streamingDecoder := streaming.NewDecoder(frameReader, streamingSerializer)
	return restclientwatch.NewDecoder(streamingDecoder, embeddedObjectDecoder), nil
}

// WatchEncode writes watch event to provided io.Writer
func (s *Serializer) WatchEncode(w io.Writer, event *watch.Event) (int, error) {
	mediaType, params, err := mime.ParseMediaType(s.contentType)
	if err != nil {
		return 0, err
	}

	// 1. prepare streaming encoder for watch event and embedded encoder for event.object
	_, streamingSerializer, framer, err := s.watchEventClientNegotiator.StreamDecoder(mediaType, params)
	if err != nil {
		return 0, err
	}

	sw := &sizeWriter{Writer: w}
	streamingEncoder := streaming.NewEncoder(framer.NewFrameWriter(sw), streamingSerializer)
	embeddedEncoder, err := s.Encoder(mediaType, params)
	if err != nil {
		return 0, err
	}

	// 2. encode the embedded object into bytes.Buffer
	buf := &bytes.Buffer{}
	obj := event.Object
	if err := embeddedEncoder.Encode(obj, buf); err != nil {
		return 0, err
	}

	// 3. make up metav1.WatchEvent and encode it by using streaming encoder
	outEvent := &metav1.WatchEvent{}
	outEvent.Type = string(event.Type)
	outEvent.Object.Raw = buf.Bytes()

	if err := streamingEncoder.Encode(outEvent); err != nil {
		return 0, err
	}

	return sw.size, nil
}

// sizeWriter used to hold total wrote bytes size
type sizeWriter struct {
	io.Writer
	size int
}

func (sw *sizeWriter) Write(p []byte) (int, error) {
	n, err := sw.Writer.Write(p)
	sw.size = sw.size + n
	klog.V(5).Infof("encode bytes data: write bytes=%d, size=%d, bytes=%v", n, sw.size, p[:n])
	return n, err
}
