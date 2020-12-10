package hevent

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/kamva/hexa"
	"github.com/kamva/tracer"
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
		// You can provide options for some drivers. e.g if you are using the nats-streaming driver, you can
		// say here that you want to close and unsubscribe or just close.
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

	// RawMessage is the message sent by emitter,
	// we will convert RawMessage to message and then
	// pass it to the event handler.
	RawMessage struct {
		Headers map[string][]byte `json:"header"`

		// Marshaller is the marshaller name to use its un-marshaller.
		Marshaller string `json:"marshaller"`

		Payload []byte `json:"payload"` // encoded data.
	}

	// Message is the message that provide to event handler.
	Message struct {
		Headers map[string][]byte

		CorrelationId string
		ReplyChannel  string

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

func (m Message) Validate() error { // TODO: I think we should remove this method.
	return validation.ValidateStruct(&m,
		validation.Field(&m.CorrelationId, validation.Required),
		validation.Field(&m.Headers, validation.Required),
		validation.Field(&m.Payload, validation.Required),
	)
}

func (e RawMessage) Validate() error {
	return validation.ValidateStruct(&e,
		validation.Field(&e.Headers, validation.Required),
		validation.Field(&e.Marshaller, validation.Required),
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
var _ validation.Validatable = &Message{}
