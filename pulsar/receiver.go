package hexapulsar

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync"
)

type (

	// receiver is implementation of the event receiver.
	receiver struct {
		client                   pulsar.Client
		consumerOptionsGenerator consumerOptionsGenerator
		consumers                []pulsar.Consumer
		wg                       *sync.WaitGroup
		done                     chan bool // on close the 'done' channel, all consumers jobs should close consumer and return.

		ctxExporterImporter hexa.ContextExporterImporter
	}

	// handlerContext implements the HandlerContext interface.
	handlerContext struct {
		context.Context
		msg pulsar.ConsumerMessage
	}
)

func (h *handlerContext) Ack() {
	h.msg.Consumer.Ack(h.msg.Message)
}

func (h *handlerContext) Nack() {
	h.msg.Consumer.Nack(h.msg.Message)
}

// Subscribe on the pulsar driver will use consumerOptionsGenerator to generate consumerOptions,
// so check it to know what will be default values of the pulsar subscription name and subscription
// type.
func (r *receiver) Subscribe(channel string, payloadInstance interface{}, h hevent.EventHandler) error {
	consumer, err := r.consumer(hevent.NewSubscriptionOptions(channel, payloadInstance, h))
	if err != nil {
		return tracer.Trace(err)
	}

	return r.subscribe(consumer, payloadInstance, h)
}

func (r *receiver) SubscribeWithOptions(so *hevent.SubscriptionOptions) error {
	if err := so.Validate(); err != nil {
		return tracer.Trace(err)
	}

	consumer, err := r.consumer(so)
	if err != nil {
		return tracer.Trace(err)
	}

	return r.subscribe(consumer, so.PayloadInstance, so.Handler)
}

func (r *receiver) Start() error {
	r.wg.Wait()
	return nil
}

func (r *receiver) Close() error {
	close(r.done)
	r.wg.Wait()
	return nil
}

func (r *receiver) subscribe(consumer pulsar.Consumer, pi interface{}, h hevent.EventHandler) error {
	r.wg.Add(1)
	go receive(consumer, r.wg, r.done, func(msg pulsar.ConsumerMessage) {
		ctx, message, err := r.extractMessage(msg, pi)
		h(newHandlerCtx(msg), ctx, message, err)
	})
	return nil
}

// consumer returns new instance of the pulsar consumer with provided channel
func (r *receiver) consumer(so *hevent.SubscriptionOptions) (pulsar.Consumer, error) {
	options, err := r.consumerOptionsGenerator.Generate(so)
	if err != nil {
		return nil, tracer.Trace(err)
	}
	return r.client.Subscribe(options)
}

func (r *receiver) extractMessage(msg pulsar.ConsumerMessage, payloadInstance interface{}) (ctx hexa.Context, m hevent.Message, err error) {
	rawMsg := hevent.RawMessage{}
	err = json.Unmarshal(msg.Message.Payload(), &rawMsg)
	if err != nil {
		err = tracer.Trace(err)
		return
	}

	// validate the message
	if err = rawMsg.Validate(); err != nil {
		err = tracer.Trace(err)
		return
	}
	// extract Context:
	ctx, err = r.ctxExporterImporter.Import(rawMsg.MessageHeader.Ctx)
	if err != nil {
		err = tracer.Trace(err)
		return
	}
	m, err = hevent.RawMessageToMessage(&rawMsg, payloadInstance)
	return
}

// receive function receive message of a consume subscription.
func receive(consumer pulsar.Consumer, wg *sync.WaitGroup, done chan bool, f func(message pulsar.ConsumerMessage)) {
	defer wg.Done()

	ch := consumer.Chan()
	for {
		select {
		case msg := <-ch:
			f(msg)
		case _ = <-done:
			consumer.Close()
			return
		}
	}
}

// newHandlerCtx returns new instance of the handler context.
func newHandlerCtx(msg pulsar.ConsumerMessage) hevent.HandlerContext {
	return &handlerContext{
		Context: context.Background(),
		msg:     msg,
	}
}

// NewReceiver returns new instance of pulsar implementation of the hexa event receiver.
func NewReceiver(client pulsar.Client, cei hexa.ContextExporterImporter) (hevent.Receiver, error) {
	if client == nil {
		return nil, tracer.Trace(errors.New("client can not be nil"))
	}

	return &receiver{
		ctxExporterImporter:      cei,
		client:                   client,
		consumerOptionsGenerator: consumerOptionsGenerator{},
		consumers:                make([]pulsar.Consumer, 0),
		wg:                       &sync.WaitGroup{},
		done:                     make(chan bool),
	}, nil
}

var _ hevent.HandlerContext = &handlerContext{}
var _ hevent.Receiver = &receiver{}
