package hevent

import (
	"encoding/json"
	"errors"

	"github.com/kamva/gutil"
	"github.com/kamva/tracer"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	// Encoder encode and decode the event payload.
	Encoder interface {
		// Return the Encoder name.
		Name() string
		Encode(interface{}) ([]byte, error)
		Decode([]byte, interface{}) error
	}

	// protobufEncoder is protobuf implementation of the Encoder
	protobufEncoder struct{}

	// jsonEncoder is json implementation of the Encoder.
	jsonEncoder struct{}
)

const (
	jsonEncoderName     = "json"
	protobufEncoderName = "protobuf"
)

var (
	protobufTypeErr = errors.New("the provided value is not protobuf message")
)

func (m jsonEncoder) Name() string {
	return jsonEncoderName
}

func (m jsonEncoder) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (m jsonEncoder) Decode(buf []byte, v interface{}) error {
	return json.Unmarshal(buf, v)
}

func (m protobufEncoder) Name() string {
	return protobufEncoderName
}

func (m protobufEncoder) Encode(v interface{}) ([]byte, error) {
	// I think we can use message v2 here.
	pb, ok := v.(proto.Message)
	if !ok {
		return nil, tracer.Trace(protobufTypeErr)
	}
	return protojson.Marshal(pb)
}

func (m protobufEncoder) Decode(buf []byte, v interface{}) error {
	pb, ok := v.(proto.Message)
	if !ok {
		return tracer.Trace(protobufTypeErr)
	}

	return protojson.Unmarshal(buf, pb)
}

// NewJsonEncoder returns new instance of the json encoder.
func NewJsonEncoder() Encoder {
	return &jsonEncoder{}
}

// NewProtobufEncoder returns new instance of the protobuf encoder.
func NewProtobufEncoder() Encoder {
	return &protobufEncoder{}
}

// NewEncoderByName returns new instance of encoder by its name
func NewEncoderByName(name string) Encoder {
	switch name {
	case protobufEncoderName:
		return NewProtobufEncoder()
	default:
		return NewJsonEncoder()
	}
}

// DecodePayloadByInstance get the payload and decode it.
func DecodePayloadByInstance(payload []byte, encoderName string, payloadInstance interface{}) (interface{}, error) {
	ecoder := NewEncoderByName(encoderName)

	v, err := gutil.ValuePtr(payloadInstance)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	err = ecoder.Decode(payload, v)
	return v, err
}

var _ Encoder = jsonEncoder{}
var _ Encoder = protobufEncoder{}
