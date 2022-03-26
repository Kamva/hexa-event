package hevent

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/kamva/hexa"
)

type (
	// Emitter is the interface to emit events
	Emitter interface {
		// Emit sends event to the channel.
		// context can be nil.
		// dont forget to validate the event here.
		Emit(context.Context, *Event) (msgID string, err error)
		hexa.Shutdownable
	}

	// Event is the event to send.
	Event struct {
		Key          string // required, can use to specify partition number.(see pulsar docs)
		Channel      string
		ReplyChannel string // optional (use if need to reply the response)
		// It will encode using either protobuf,json,... encoder(relative to config of emitter).
		// Dont forget that your emitter encoder and event receivers decoder should match with each other.
		Payload interface{}
	}
)

func (e Event) Validate() error {
	return validation.ValidateStruct(&e,
		validation.Field(&e.Channel, validation.Required),
		validation.Field(&e.Key, validation.Required),
	)
}

var _ validation.Validatable = &Event{}
