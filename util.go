package hevent

import (
	"github.com/Kamva/hexa"
	"github.com/Kamva/tracer"
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
