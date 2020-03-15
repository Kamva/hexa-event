package hevent

import (
	"context"
	"github.com/Kamva/hexa"
	validation "github.com/go-ozzo/ozzo-validation"
)

type (
	// Emitter is the interface to emit events
	Emitter interface {
		// Emit send event to the channel.
		// context can be nil.
		// dont forget to validate the event here.
		Emit(hexa.Context, *Event) (msgID string, err error)

		// EmitWithCtx is just same as Emit, but need to context.
		EmitWithCtx(context.Context, hexa.Context, *Event) (msgID string, err error)

		// Close close the connection
		Close() error
	}

	// Event is the event to send.
	Event struct {
		Key          string // required, can use to specify partition number.(see pulsar docs)
		Channel      string
		ReplyChannel string // optional (use if need to reply the response)
		// It will marshall using either protobuf,json,... marshaller(relative to config of emitter).
		// Dont forget that your emitter marshaller and event receivers un-marshaller should match with each other.
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
