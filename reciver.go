package hevent

import (
	"context"
	"github.com/Kamva/hexa"
	"github.com/Kamva/tracer"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type (
	// SubscriptionOptions contains options to subscribe to one or multiple channels.
	SubscriptionOptions struct {
		// Channel specify the channel name you will subscribe on.
		// Either Channel,Channels or ChannelsPattern are required when subscribing.
		Channel string

		// Channels contains name of channels which we want to subscribe.
		// Either Channel,Channels or ChannelsPattern are required when subscribing.
		Channels []string

		// ChannelsPattern is the pattern you will use to subscribe on all channels
		// which match with this pattern.
		// Either Channel,Channels or ChannelsPattern are required when subscribing.
		ChannelsPattern string

		// PayloadInstance is the instance of event payload.
		PayloadInstance interface{}

		// Handler is the event handler.
		Handler EventHandler

		// extra contains extra details for specific drivers(e.g for pulsar you can set extra consumer options here).
		extra []interface{}
	}

	// EventHandler handle events.
	EventHandler func(HandlerContext, hexa.Context, Message, error)

	Receiver interface {
		// Subscribe subscribe to the provided channel
		Subscribe(channel string, payloadInstance interface{}, h EventHandler) error

		// SubscribeWithOptions subscribe by options.
		SubscribeWithOptions(*SubscriptionOptions) error

		// Start starts receiving the messages.
		Start() error

		// Close closes the connection.
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

func (so *SubscriptionOptions) Validate() error {
	err := validation.ValidateStruct(so,
		validation.Field(&so.Channel, validation.Required.When(len(so.Channels) == 0 && so.ChannelsPattern == "")),
		validation.Field(&so.ChannelsPattern, validation.Required.When(so.Channel == "" && len(so.Channels) == 0)),
		validation.Field(
			&so.Channels,
			validation.Required.When(so.Channel == "" && so.ChannelsPattern == ""),
			validation.Each(validation.Required),
		),
	)
	return tracer.Trace(err)
}

// WithExtra add Extra data to the subscription options.
func (so *SubscriptionOptions) WithExtra(extra ...interface{}) *SubscriptionOptions {
	so.extra = append(so.extra, extra...)
	return so
}

// Extra returns the extra data of the subscription options.
func (so *SubscriptionOptions) Extra() []interface{} {
	return so.extra
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

// NewSubscriptionOptions returns new instance of the subscription options.
func NewSubscriptionOptions(channel string, payloadInstance interface{}, handler EventHandler) *SubscriptionOptions {
	return &SubscriptionOptions{
		Channel:         channel,
		PayloadInstance: payloadInstance,
		Handler:         handler,
	}
}

// Assertion
var _ validation.Validatable = &SubscriptionOptions{}
var _ validation.Validatable = &MessageHeader{}
var _ validation.Validatable = &Message{}
