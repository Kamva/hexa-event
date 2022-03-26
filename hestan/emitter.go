// Package hestan (hexa stan) in implementation of Nats-streaming
// broker for hexa SDK using stan client library of NATS.
package hestan

import (
	"context"
	"encoding/json"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

type EmitterOptions struct {
	NatsCon           *nats.Conn
	StreamingCon      stan.Conn
	ContextPropagator hexa.ContextPropagator
	Encoder           hevent.Encoder
}

func (o EmitterOptions) Validate() error {
	return validation.ValidateStruct(&o,
		validation.Field(&o.NatsCon, validation.Required),
		validation.Field(&o.StreamingCon, validation.Required),
		validation.Field(&o.ContextPropagator, validation.Required),
		validation.Field(&o.Encoder, validation.Required),
	)
}

type emitter struct {
	nc           *nats.Conn
	sc           stan.Conn
	p            hexa.ContextPropagator
	msgConverter hevent.RawMessageConverter
}

func (e *emitter) Emit(ctx context.Context, event *hevent.Event) (msgID string, err error) {
	// TODO: implement publish async option also.

	if err := event.Validate(); err != nil {
		return "", tracer.Trace(err)
	}

	raw, err := e.msgConverter.EventToRaw(ctx, event)
	if err != nil {
		return "", tracer.Trace(err)
	}

	payload, err := json.Marshal(raw)
	if err != nil {
		return "", tracer.Trace(err)
	}

	hlog.Debug("emit event",
		hlog.String("channel", event.Channel),
		hlog.String("payload", string(payload)),
	)

	return "", tracer.Trace(e.sc.Publish(event.Channel, payload))
}

func (e *emitter) Shutdown(_ context.Context) error {
	defer e.nc.Close()
	return tracer.Trace(e.sc.Close())
}

// NewEmitter returns new emitter with tha nats-streaming driver.
func NewEmitter(o EmitterOptions) (hevent.Emitter, error) {
	return &emitter{
		nc:           o.NatsCon,
		sc:           o.StreamingCon,
		p:            o.ContextPropagator,
		msgConverter: hevent.NewRawMessageConverter(o.ContextPropagator, o.Encoder),
	}, o.Validate()
}

var _ hevent.Emitter = &emitter{}
