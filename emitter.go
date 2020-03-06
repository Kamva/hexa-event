package kevent

import (
	"context"
	"github.com/Kamva/kitty"
	validation "github.com/go-ozzo/ozzo-validation"
)

type (
	// Emitter is the interface to emit events
	Emitter interface {
		// Emit send event to the channel.
		// context can be nil.
		// dont forget to validate the event here.
		Emit(context.Context, *Event) (msgID string, err error)

		// Close close the connection
		Close() error
	}

	// Header is the event Header
	Header struct {
		RequestID     string    `json:"request_id"`     // optional
		CorrelationID string    `json:"correlation_id"` //required
		ReplyChannel  string    `json:"reply_channel"`  // optional (use if need to reply the response)
		Ctx           kitty.Map `json:"ctx"`            // extract context as map
	}

	// Event is the event to send.
	Event struct {
		Header  `json:"header"`
		Payload kitty.Map `json:"payload"`

		Channel string `json:"-"` // required
		Key     string `json:"-"` // required, can use to select event partition (e.g in pulsar & kafka).
	}
)

func (h Header) Validate() error {
	return validation.ValidateStruct(&h,
		validation.Field(&h.CorrelationID, validation.Required),
		validation.Field(&h.Ctx, validation.Required),
	)
}

func (e Event) Validate() error {
	return validation.ValidateStruct(&e,
		validation.Field(&e.Channel, validation.Required),
		validation.Field(&e.Key, validation.Required),
		validation.Field(&e.Header, validation.Required),
		validation.Field(&e.Payload, validation.Required),
	)
}
