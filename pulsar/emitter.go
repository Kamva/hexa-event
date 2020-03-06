//---------------------------------------
// Pulsar implementation of kitty events.
// Pulsar driver is thread safe.
//---------------------------------------
package kpulsar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Kamva/tracer"
	"github.com/Kavma/kitty-event"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync"
	"time"
)

type (
	// ProducerGenerator generate new producer
	ProducerGenerator func(c pulsar.Client, topic string) (pulsar.Producer, error)

	// pulsar implementation of the kitty Emitter.
	emitter struct {
		client    pulsar.Client
		pg        ProducerGenerator
		producers map[string]pulsar.Producer
		l         *sync.RWMutex
	}
)

func (e *emitter) ctx(c context.Context) context.Context {
	if c == nil {
		c = context.Background()
	}

	return c
}

func (e *emitter) Emit(c context.Context, event *kevent.Event) (string, error) {
	c = e.ctx(c)
	if err := event.Validate(); err != nil {
		return "", tracer.Trace(err)
	}

	p, err := e.producer(event.Channel)
	if err != nil {
		return "", tracer.Trace(err)
	}

	msg, err := e.msg(event)
	if err != nil {
		return "", tracer.Trace(err)
	}

	id, err := p.Send(c, msg)
	if err != nil {
		return "", tracer.Trace(err)
	}

	return string(id.Serialize()), tracer.Trace(err)
}

func (e *emitter) msg(event *kevent.Event) (*pulsar.ProducerMessage, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		err = tracer.Trace(err)
		return nil, err
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

	// generate new producer
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
// topicPattern is the topic name pattern, e.g "persistent://public/default/%s", and
// then we set the provided topic name in the topic pattern.
func CustomProducerGenerator(topicPattern string, options pulsar.ProducerOptions) ProducerGenerator {
	return func(client pulsar.Client, topic string) (producer pulsar.Producer, err error) {
		options.Topic = fmt.Sprintf(topicPattern, topic)
		return client.CreateProducer(options)
	}
}

// NewEmitter returns new instance of pulsar emitter
func NewEmitter(client pulsar.Client, pg ProducerGenerator) (kevent.Emitter, error) {
	if client == nil {
		return nil, tracer.Trace(errors.New("client can not be nil"))
	}

	return &emitter{
		client:    client,
		pg:        pg,
		producers: make(map[string]pulsar.Producer),
		l:         &sync.RWMutex{},
	}, nil
}

var _ kevent.Emitter = &emitter{}
