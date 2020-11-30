// hestan (hexa stan) in implementation of Nats-streaming
// broker for hexa SDK using stan client library of NATS.
package hestan

import (
	"context"
	"encoding/json"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa-event/internal/helper"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

type EmitterOptions struct {
	NatsCon             *nats.Conn
	StreamingCon        stan.Conn
	CtxExporterImporter hexa.ContextExporterImporter
	Marshaller          hevent.Marshaller
}

func (o EmitterOptions) Validate() error {
	return validation.ValidateStruct(&o,
		validation.Field(&o.NatsCon, validation.Required),
		validation.Field(&o.StreamingCon, validation.Required),
		validation.Field(&o.CtxExporterImporter, validation.Required),
		validation.Field(&o.Marshaller, validation.Required),
	)
}

type emitter struct {
	nc         *nats.Conn
	sc         stan.Conn
	cei        hexa.ContextExporterImporter
	marshaller hevent.Marshaller
}

func (e *emitter) Emit(c hexa.Context, event *hevent.Event) (msgID string, err error) {
	return e.EmitWithCtx(helper.Ctx(nil), c, event)
}

func (e *emitter) EmitWithCtx(c context.Context, ctx hexa.Context, event *hevent.Event) (msgID string, err error) {
	// TODO: implement publish async option also.
	raw, err := helper.EventToRawMessage(ctx, event, e.cei, e.marshaller)
	if err != nil {
		return "", tracer.Trace(err)
	}

	payload, err := json.Marshal(raw)
	if err != nil {
		return "", tracer.Trace(err)
	}

	hlog.With(hlog.String("channel", event.Channel), hlog.String("payload", string(payload))).Debug("emit event")
	return "", tracer.Trace(e.sc.Publish(event.Channel, payload))
}

func (e *emitter) Close() error {
	defer e.nc.Close()
	return tracer.Trace(e.sc.Close())
}

// NewEmitter returns new emitter with tha nats-streaming driver.
func NewEmitter(o EmitterOptions) (hevent.Emitter, error) {
	return &emitter{
		nc:         o.NatsCon,
		sc:         o.StreamingCon,
		cei:        o.CtxExporterImporter,
		marshaller: o.Marshaller,
	}, o.Validate()
}

var _ hevent.Emitter = &emitter{}
