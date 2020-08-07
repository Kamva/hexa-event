package hevent

import (
	"encoding/json"
	"errors"
	"github.com/kamva/gutil"
	"github.com/kamva/tracer"
	"github.com/golang/protobuf/proto"
)

type (
	// Marshaller is the marshaller that marshal and unmarshal the event payload.
	Marshaller interface {
		// Return the marshaller name.
		Name() string
		Marshal(interface{}) ([]byte, error)
		Unmarshal([]byte, interface{}) error
	}

	// protobufMarshaller is protobuf implementation of the Marshaller
	protobufMarshaller struct{}

	// jsonMarshaller is json implementation of the Marshaller.
	jsonMarshaller struct{}
)

const (
	jsonMarshallerName     = "json"
	protobufMarshallerName = "protobuf"
)

var (
	protobufTypeErr = errors.New("the provided value is not protobuf message")
)

func (m jsonMarshaller) Name() string {
	return jsonMarshallerName
}

func (m jsonMarshaller) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (m jsonMarshaller) Unmarshal(buf []byte, v interface{}) error {
	return json.Unmarshal(buf, v)
}

func (m protobufMarshaller) Name() string {
	return protobufMarshallerName
}

func (m protobufMarshaller) Marshal(v interface{}) ([]byte, error) {
	pb, ok := v.(proto.Message)
	if !ok {
		return nil, tracer.Trace(protobufTypeErr)
	}
	return proto.Marshal(pb)
}

func (m protobufMarshaller) Unmarshal(buf []byte, v interface{}) error {
	pb, ok := v.(proto.Message)
	if !ok {
		return tracer.Trace(protobufTypeErr)
	}

	return proto.Unmarshal(buf, pb)
}

// NewJsonMarshaller returns new instance of the json marshaller.
func NewJsonMarshaller() Marshaller {
	return &jsonMarshaller{}
}

// NewProtobufMarshaller returns new instance of the protobuf marshaller.
func NewProtobufMarshaller() Marshaller {
	return &protobufMarshaller{}
}

// NewMarshallerByName returns new instance of marshaller by its name
func NewMarshallerByName(name string) Marshaller {
	switch name {
	case protobufMarshallerName:
		return NewProtobufMarshaller()
	default:
		return NewJsonMarshaller()
	}
}

// UnmarshalPayloadByInstance get the payload and unmarshal it.
func UnmarshalPayloadByInstance(payload []byte, marshallerName string, payloadInstance interface{}) (interface{}, error) {
	marshaller := NewMarshallerByName(marshallerName)

	v, err := gutil.ValuePtr(payloadInstance)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	err = marshaller.Unmarshal(payload, v)
	return v, err
}

var _ Marshaller = jsonMarshaller{}
var _ Marshaller = protobufMarshaller{}
