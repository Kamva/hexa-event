package hevent

import (
	"context"
	"errors"

	"github.com/kamva/hexa"
	"github.com/kamva/tracer"
)

const (
	HeaderKeyReplyChannel   = "_reply_channel"
	HeaderKeyPayloadEncoder = "_payload_encoder" // the message body.
)

type RawMessageConverter interface {
	EventToRaw(c hexa.Context, e *Event) (*RawMessage, error)
	RawMsgToMessage(c context.Context, raw *RawMessage) (hexa.Context, Message, error)
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

	if err != nil {
		return nil, tracer.Trace(err)
	}

	headers, err := m.p.Extract(ctx)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	headers[HeaderKeyReplyChannel] = []byte(event.ReplyChannel)
	headers[HeaderKeyPayloadEncoder] = []byte(m.e.Name())

	return &RawMessage{
		Headers: headers,
		Payload: payload,
	}, err
}

func (m *rawMessageConverter) RawMsgToMessage(c context.Context, rawMsg *RawMessage) (
	ctx hexa.Context, msg Message, err error) {

	c, err = m.p.Inject(rawMsg.Headers, c)
	if err != nil {
		err = tracer.Trace(err)
		return
	}

	ctx = hexa.MustNewContextFromRawContext(c)
	encoderName := string(rawMsg.Headers[HeaderKeyPayloadEncoder])
	encoder, ok := encoders[encoderName]
	if !ok {
		err = errors.New("can not find message payload's encoder/decoder")
		return
	}

	msg = Message{
		Headers:       rawMsg.Headers,
		CorrelationId: ctx.CorrelationID(),
		ReplyChannel:  string(rawMsg.Headers[HeaderKeyReplyChannel]),
		Payload:       encoder.Decoder(rawMsg.Payload),
	}
	return
}

var _ RawMessageConverter = &rawMessageConverter{}
