package hafka

import (
	"github.com/Shopify/sarama"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

type EmitterOptions struct {
	Producer          sarama.AsyncProducer
	ContextPropagator hexa.ContextPropagator
	Encoder           hevent.Encoder
}

func (o EmitterOptions) Validate() error {
	return validation.ValidateStruct(&o,
		validation.Field(&o.Producer, validation.Required),
		validation.Field(&o.ContextPropagator, validation.Required),
		validation.Field(&o.Encoder, validation.Required),
	)
}

type emitter struct {
	producer     sarama.AsyncProducer
	p            hexa.ContextPropagator
	msgConverter MessageConverter
}

func NewEmitter(o EmitterOptions) (hevent.Emitter, error) {
	if err := o.Validate(); err != nil {
		return nil, tracer.Trace(err)
	}

	rawMsgConverter := hevent.NewRawMessageConverter(o.ContextPropagator, o.Encoder)

	return &emitter{
		producer:     o.Producer,
		p:            o.ContextPropagator,
		msgConverter: newMessageConverter(rawMsgConverter),
	}, nil
}

func (e *emitter) Emit(ctx hexa.Context, event *hevent.Event) (msgID string, err error) {
	if err := event.Validate(); err != nil {
		return "", tracer.Trace(err)
	}

	msg, err := e.msgConverter.EventToProducerMessage(ctx, event)
	if err != nil {
		return "", tracer.Trace(err)
	}

	if err := e.logMessage(msg); err != nil {
		return "", tracer.Trace(err)
	}

	e.producer.Input() <- msg

	return "", nil
}

func (e *emitter) logMessage(msg *sarama.ProducerMessage) error {
	key, err := msg.Key.Encode()
	if err != nil {
		return tracer.Trace(err)
	}

	val, err := msg.Value.Encode()
	if err != nil {
		return tracer.Trace(err)
	}

	hlog.Debug("emit kafka event",
		hlog.String("topic", msg.Topic),
		hlog.Time("payload", msg.Timestamp),
		hlog.String("key", string(key)),
		hlog.String("payload", string(val)),
	)

	return nil
}

func (e *emitter) Close() error {
	e.producer.AsyncClose()
	return nil
}

var _ hevent.Emitter = &emitter{}
