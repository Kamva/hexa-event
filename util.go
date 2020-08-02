package hevent

import (
	"github.com/kamva/hexa"
	"github.com/kamva/tracer"
)

// EventToRawMessage converts Event instance to Message.
func EventToRawMessage(ctx hexa.Context, event *Event, ce hexa.ContextExporterImporter, marshaller Marshaller) (*RawMessage, error) {
	payload, err := marshaller.Marshal(event.Payload)
	exportedCtx, err := ce.Export(ctx)
	if err != nil {
		return nil, tracer.Trace(err)
	}
	return &RawMessage{
		MessageHeader: MessageHeader{
			CorrelationID: ctx.CorrelationID(),
			ReplyChannel:  event.ReplyChannel,
			Ctx:           exportedCtx,
		},
		Marshaller: marshaller.Name(),
		Payload:    payload,
	}, err
}

func RawMessageToMessage(rawMsg *RawMessage, payloadInstance interface{}) (Message, error) {
	v, err := UnmarshalPayloadByInstance(rawMsg.Payload, rawMsg.Marshaller, payloadInstance)

	return Message{
		MessageHeader: rawMsg.MessageHeader,
		Payload:       v,
	}, err
}

// SubscribeMulti subscribes to multiple channel.
func SubscribeMulti(r Receiver, options ...*SubscriptionOptions) error {
	for _, o := range options {
		if err := r.SubscribeWithOptions(o); err != nil {
			return err
		}
	}

	return nil
}
