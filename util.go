package hevent

import (
	"github.com/Kamva/gutil"
	"github.com/Kamva/hexa"
	"github.com/Kamva/tracer"
)

type (
	// SubscriptionItem contains required params to subscribe to a channel.
	SubscriptionItem struct {
		Channel ChannelNames
		//We set this options here , so you by defining just a simple subscription Item, can set both
		//pulsar consumer options and handler.
		Handler         EventHandler
		PayloadInstance interface{} // instance can be just empty pointer instance of the payload.
	}
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

// DefaultSubscriptionItem returns new instance of Subscription Item by default values.
func DefaultSubscriptionItem(channel string, instance interface{}, handler EventHandler) SubscriptionItem {
	return SubscriptionItem{
		Channel:         NewChannelNames(channel, channel),
		Handler:         handler,
		PayloadInstance: instance,
	}
}

// SubscribeByList subscribe to multiple event by provided list.
func SubscribeByList(r Receiver, subscriptionItems []SubscriptionItem) {
	for _, event := range subscriptionItems {
		err := r.SubscribeMulti(event.Channel, event.PayloadInstance, event.Handler)
		gutil.PanicErr(err)
	}
}
