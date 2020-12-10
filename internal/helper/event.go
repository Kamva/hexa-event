package helper

import (
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
)

const (
	CorrelationIdHeaderKey = "_correlation_id"
	ReplyChannelHeaderKey  = "_reply_channel"
)

// EventToRawMessage converts Event instance to Message.
func EventToRawMessage(ctx hexa.Context, event *hevent.Event, p hexa.ContextPropagator, e hevent.Encoder) (
	*hevent.RawMessage, error) {
	payload, err := e.Encode(event.Payload)

	headers, err := p.Extract(ctx)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	headers[CorrelationIdHeaderKey] = []byte(ctx.CorrelationID())
	headers[ReplyChannelHeaderKey] = []byte(event.ReplyChannel)

	return &hevent.RawMessage{
		Headers:    headers,
		Marshaller: e.Name(),
		Payload:    payload,
	}, err
}

func RawMessageToMessage(rawMsg *hevent.RawMessage, payloadInstance interface{}) (hevent.Message, error) {
	p, err := hevent.DecodePayloadByInstance(rawMsg.Payload, rawMsg.Marshaller, payloadInstance)

	return hevent.Message{
		CorrelationId: string(rawMsg.Headers[CorrelationIdHeaderKey]),
		ReplyChannel:  string(rawMsg.Headers[ReplyChannelHeaderKey]),
		Payload:       p,
	}, tracer.Trace(err)
}
