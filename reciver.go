package hevent

import (
	"context"
	"github.com/Kamva/hexa"
	"github.com/Kamva/tracer"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type (
	// ChannelNames contains description about the channel names
	// that provide to the consumerGenerator to generate consumer.
	ChannelNames struct {
		// Name of the subscription.subscriptionName group listeners
		// of a the channel. see some message-brokers like pulsar to
		// detect usage.
		SubscriptionName string

		// If Names is empty, so you should check pattern
		Names   []string
		Pattern string
	}

	// EventHandler handle events.
	EventHandler func(HandlerContext, hexa.Context, Message, error)

	Receiver interface {
		// Subscribe subscribe to the provided channel
		Subscribe(subscriptionName string, channel string, payloadInstance interface{}, h EventHandler) error
		// SubscribeMulti subscribe to multiple channel by either providing list of channels or pattern.
		SubscribeMulti(chNames ChannelNames, payloadInstance interface{}, h EventHandler) error

		// Start starts receiving the messages.
		Start() error

		// Close close the connection
		Close() error
	}

	// HandlerContext is the context that pass to the message handler.
	HandlerContext interface {
		context.Context
		// Ack get the message and send ack.
		Ack()
		// Ack get the message and send negative ack.
		Nack()
	}

	// MessageHeader is header of the message.
	MessageHeader struct {
		CorrelationID string   `json:"correlation_id"` // required
		ReplyChannel  string   `json:"reply_channel"`  // optional (use if need to reply the response)
		Ctx           hexa.Map `json:"ctx"`            // extract context as map
	}

	// RawMssage is the message sent by emitter,
	// we will convert RawMssage to message and then
	// pass it to the event handler.
	RawMessage struct {
		MessageHeader `json:"header"`

		// Marshaller is the marshaller name to use its un-marshaller.
		Marshaller string `json:"marshaller"`

		Payload []byte `json:"payload"` // marshalled protobuf or marshalled json.
	}

	// Message is the message that provide to event handler.
	Message struct {
		MessageHeader
		// type of payload is same as provided payload
		// instance that provide on subscription.
		Payload interface{}
	}
)

func (cn ChannelNames) Validate() error {
	err := validation.ValidateStruct(&cn,
		validation.Field(&cn.Names, validation.Required.When(cn.Pattern == ""), validation.Each(validation.Required)),
		validation.Field(&cn.Pattern, validation.Required.When(len(cn.Names) == 0)),
	)
	return tracer.Trace(err)
}

// NewChannelNames return new instance of the ChannelNames strut.
func NewChannelNames(subscriptionName string, names ...string) ChannelNames {
	c := ChannelNames{
		SubscriptionName: subscriptionName,
		Names:            names,
	}

	return c
}

// NewChannelPattern return new instance of ChanelNames that contains pattern.
func NewChannelPattern(subscriptionName string, pattern string) ChannelNames {
	c := ChannelNames{
		SubscriptionName: subscriptionName,
		Pattern:          pattern,
	}

	return c
}

func (h MessageHeader) Validate() error {
	return validation.ValidateStruct(&h,
		validation.Field(&h.CorrelationID, validation.Required),
		validation.Field(&h.Ctx, validation.Required),
	)
}

func (e RawMessage) Validate() error {
	return validation.ValidateStruct(&e,
		validation.Field(&e.MessageHeader, validation.Required),
	)
}

var _ validation.Validatable = &ChannelNames{}
var _ validation.Validatable = &MessageHeader{}
var _ validation.Validatable = &Message{}
