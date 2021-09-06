package kafkabox

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

type EmitterOptions struct {
	Outbox            OutboxStore
	ContextPropagator hexa.ContextPropagator
	Encoder           hevent.Encoder
}

func (o EmitterOptions) Validate() error {
	return validation.ValidateStruct(&o,
		validation.Field(&o.Outbox, validation.Required),
		validation.Field(&o.ContextPropagator, validation.Required),
		validation.Field(&o.Encoder, validation.Required),
	)
}

type emitter struct {
	outbox       OutboxStore
	p            hexa.ContextPropagator
	msgConverter MessageConverter
}

func (e *emitter) HealthIdentifier() string {
	return "kafka_outbox_emitter"
}

func (e *emitter) LivenessStatus(ctx context.Context) hexa.LivenessStatus {
	if err := e.outbox.Ping(ctx); err != nil {
		hlog.Error("error on ping to the kafka outbox store", hlog.ErrStack(tracer.Trace(err)), hlog.Err(err))
		return hexa.StatusDead
	}
	return hexa.StatusAlive
}

func (e *emitter) ReadinessStatus(ctx context.Context) hexa.ReadinessStatus {
	if err := e.outbox.Ping(ctx); err != nil {
		hlog.Error("error on ping to the kafka outbox store", hlog.ErrStack(tracer.Trace(err)), hlog.Err(err))
		return hexa.StatusUnReady
	}
	return hexa.StatusReady
}

func (e *emitter) HealthStatus(ctx context.Context) hexa.HealthStatus {
	return hexa.HealthStatus{
		Id:    e.HealthIdentifier(),
		Alive: e.LivenessStatus(ctx),
		Ready: e.ReadinessStatus(ctx),
	}
}

func NewEmitter(o EmitterOptions) (hevent.Emitter, error) {
	if err := o.Validate(); err != nil {
		return nil, tracer.Trace(err)
	}

	rawMsgConverter := hevent.NewRawMessageConverter(o.ContextPropagator, o.Encoder)

	return &emitter{
		outbox:       o.Outbox,
		p:            o.ContextPropagator,
		msgConverter: newMessageConverter(rawMsgConverter),
	}, nil
}

func (e *emitter) Emit(ctx hexa.Context, event *hevent.Event) (msgID string, err error) {
	if err := event.Validate(); err != nil {
		return "", tracer.Trace(err)
	}

	msg, err := e.msgConverter.EventToOutboxMessage(ctx, event)
	if err != nil {
		return "", tracer.Trace(err)
	}

	if err := e.logMessage(msg); err != nil {
		return "", tracer.Trace(err)
	}

	return msg.ID, tracer.Trace(e.outbox.Create(ctx, msg))
}

func (e *emitter) logMessage(msg *OutboxMessage) error {
	hlog.Debug("emitting kafka event",
		hlog.String("topic", msg.Topic),
		hlog.String("key", msg.Key),
		hlog.String("value", msg.Value),
		hlog.Any("headers", msg.Headers),
	)

	return nil
}

func (e *emitter) Shutdown(c context.Context) error {
	return tracer.Trace(e.outbox.Shutdown(c))
}

var _ hevent.Emitter = &emitter{}
var _ hexa.Health = &emitter{}
