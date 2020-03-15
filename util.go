package hevent

import (
	"github.com/Kamva/hexa"
)

// EventToRawMessage converts Event instance to Message.
func EventToRawMessage(ctx hexa.Context, event *Event, marshaller Marshaller) (*RawMessage, error) {
	payload, err := marshaller.Marshal(event.Payload)
	return &RawMessage{
		MessageHeader: MessageHeader{
			CorrelationID: ctx.CorrelationID(),
			ReplyChannel:  event.ReplyChannel,
			Ctx:           ctx.ToMap(),
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
