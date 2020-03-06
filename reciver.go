package kevent

import (
	"context"
	"github.com/Kamva/kitty"
	"github.com/Kamva/tracer"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type (
	// Topic is descriptions about the topic(especially topic's Name)
	// that provide to the consumerGenerator to generate consumer.
	ChannelNames struct {
		SubscriptionName string
		Names            []string // If Names is empty, so you should check pattern.
		Pattern          string
	}

	// EventHandler handle events.
	EventHandler func(HandlerContext, kitty.Context, Message, error)

	Receiver interface {
		// Subscribe subscribe to the provided channel
		Subscribe(subscriptionName string, channel string, h EventHandler) error
		// SubscribeMulti subscribe to multiple channel by either providing list of channels or pattern.
		SubscribeMulti(ChannelNames, EventHandler) error

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

	Message = Event
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

var _ validation.Validatable = &ChannelNames{}
