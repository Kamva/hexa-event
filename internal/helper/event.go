package helper

import (
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
)

// EventToRawMessage converts Event instance to Message.
func EventToRawMessage(ctx hexa.Context, event *hevent.Event, ce hexa.ContextExporterImporter, marshaller hevent.Marshaller) (
	*hevent.RawMessage, error) {
	payload, err := marshaller.Marshal(event.Payload)
	exportedCtx, err := ce.Export(ctx)
	if err != nil {
		return nil, tracer.Trace(err)
	}
	return &hevent.RawMessage{
		MessageHeader: hevent.MessageHeader{
			CorrelationID: ctx.CorrelationID(),
			ReplyChannel:  event.ReplyChannel,
			Ctx:           exportedCtx,
		},
		Marshaller: marshaller.Name(),
		Payload:    payload,
	}, err
}

func RawMessageToMessage(rawMsg *hevent.RawMessage, payloadInstance interface{}) (hevent.Message, error) {
	v, err := hevent.UnmarshalPayloadByInstance(rawMsg.Payload, rawMsg.Marshaller, payloadInstance)

	return hevent.Message{
		MessageHeader: rawMsg.MessageHeader,
		Payload:       v,
	}, err
}
