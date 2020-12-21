//---------------------------------------
// Pulsar implementation of hexa events.
// Pulsar driver is thread safe.
//---------------------------------------
package hexapulsar

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/kamva/hexa"
	"github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
)

type (
	// EmitterOptions contains options that can provide on create emitter.
	EmitterOptions struct {
		ProducerGenerator ProducerGenerator
		ContextPropagator hexa.ContextPropagator
		Encoder           hevent.Encoder
	}

	// ProducerGenerator setOptionValues new producer
	ProducerGenerator func(c pulsar.Client, topic string) (pulsar.Producer, error)

	// pulsar implementation of the hexa Emitter.
	emitter struct {
		client    pulsar.Client
		pg        ProducerGenerator
		producers map[string]pulsar.Producer
		l         *sync.RWMutex

		p            hexa.ContextPropagator
		msgConverter hevent.RawMessageConverter
	}
)

func (e *emitter) Emit(ctx hexa.Context, event *hevent.Event) (string, error) {

	if err := event.Validate(); err != nil {
		return "", tracer.Trace(err)
	}

	p, err := e.producer(event.Channel)
	if err != nil {
		return "", tracer.Trace(err)
	}

	msg, err := e.msg(ctx, event)
	if err != nil {
		return "", tracer.Trace(err)
	}

	id, err := p.Send(ctx, msg)
	if err != nil {
		return "", tracer.Trace(err)
	}

	return string(id.Serialize()), tracer.Trace(err)
}

func (e *emitter) msg(ctx hexa.Context, event *hevent.Event) (*pulsar.ProducerMessage, error) {
	msg, err := e.msgConverter.EventToRaw(ctx, event)
	if err != nil {
		return nil, tracer.Trace(err)
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	return &pulsar.ProducerMessage{
		Key:       event.Key,
		Payload:   payload,
		EventTime: time.Now(),
	}, nil
}

func (e *emitter) producer(topic string) (pulsar.Producer, error) {
	e.l.Lock()
	defer e.l.Unlock()

	if p, ok := e.producers[topic]; ok {
		return p, nil
	}

	// setOptionValues new producer
	p, err := e.pg(e.client, topic)
	e.producers[topic] = p
	return p, tracer.Trace(err)
}

func (e *emitter) Close() error {
	e.l.Lock()
	defer e.l.Unlock()
	for _, p := range e.producers {
		p.Close()
	}

	e.client.Close()

	return nil
}

// DefaultProducerGenerator returns producer generator function
// with default options.
// topicPattern is the topic name pattern, e.g "persistent://public/default/%s", and
// then we set the provided topic name in the topic pattern.
func DefaultProducerGenerator(topicPattern string) ProducerGenerator {
	return CustomProducerGenerator(topicPattern, pulsar.ProducerOptions{})
}

// CustomProducerGenerator gets producer options and just set the topic name on the producer.
// then returns new producer.
// topicFormat is the topic name format, e.g "persistent://public/default/%s", and
// then we set the provided topic name in the topic pattern.
func CustomProducerGenerator(topicFormat string, options pulsar.ProducerOptions) ProducerGenerator {
	return func(client pulsar.Client, topic string) (producer pulsar.Producer, err error) {
		options.Topic = fmt.Sprintf(topicFormat, topic)
		return client.CreateProducer(options)
	}
}

// NewEmitter returns new instance of pulsar emitter
func NewEmitter(client pulsar.Client, options EmitterOptions) (hevent.Emitter, error) {
	if client == nil {
		return nil, tracer.Trace(errors.New("client can not be nil"))
	}

	return &emitter{
		client:       client,
		pg:           options.ProducerGenerator,
		producers:    make(map[string]pulsar.Producer),
		l:            &sync.RWMutex{},
		p:            options.ContextPropagator,
		msgConverter: hevent.NewRawMessageConverter(options.ContextPropagator, options.Encoder),
	}, nil
}

// Assertion
var _ hevent.Emitter = &emitter{}
