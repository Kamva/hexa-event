package hevent

import (
	"context"

	"github.com/kamva/hexa"
	"github.com/kamva/tracer"
)

const (
	ReplyChannelHeaderKey = "_reply_channel"
)

type RawMessageConverter interface {
	EventToRaw(c hexa.Context, e *Event) (*RawMessage, error)
	RawMsgToMessage(c context.Context, raw *RawMessage, payloadInstance interface{}) (hexa.Context, Message, error)
}

type rawMessageConverter struct {
	p hexa.ContextPropagator
	e Encoder
}

func NewRawMessageConverter(p hexa.ContextPropagator, e Encoder) RawMessageConverter {
	return &rawMessageConverter{
		p: p,
		e: e,
	}
}

func (m *rawMessageConverter) EventToRaw(ctx hexa.Context, event *Event) (*RawMessage, error) {
	payload, err := m.e.Encode(event.Payload)

	headers, err := m.p.Extract(ctx)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	headers[ReplyChannelHeaderKey] = []byte(event.ReplyChannel)

	return &RawMessage{
		Headers:    headers,
		Marshaller: m.e.Name(),
		Payload:    payload,
	}, err
}

func (m *rawMessageConverter) RawMsgToMessage(c context.Context, rawMsg *RawMessage, payloadInstance interface{}) (
	ctx hexa.Context, msg Message, err error) {

	c, err = m.p.Inject(rawMsg.Headers, c)
	if err != nil {
		err = tracer.Trace(err)
		return
	}

	ctx = hexa.MustNewContextFromRawContext(c)

	var p interface{}
	p, err = DecodePayloadByInstance(rawMsg.Payload, rawMsg.Marshaller, payloadInstance)

	msg = Message{
		Headers:       rawMsg.Headers,
		CorrelationId: ctx.CorrelationID(),
		ReplyChannel:  string(rawMsg.Headers[ReplyChannelHeaderKey]),
		Payload:       p,
	}
	return
}

var _ RawMessageConverter = &rawMessageConverter{}
