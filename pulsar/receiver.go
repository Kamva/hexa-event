package hexapulsar

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Kamva/gutil"
	"github.com/Kamva/hexa"
	hevent "github.com/Kamva/hexa-event"
	"github.com/Kamva/tracer"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync"
)

type (

	// ReceiverOptions is the options to create a new receiver instance.
	ReceiverOptions struct {
		CtxImporter              hexa.ContextExporterImporter
		ConsumerOptionsGenerator ConsumerOptionsGenerator
	}


	// receiver is implementation of the event receiver.
	receiver struct {
		client            pulsar.Client
		cg                ConsumerOptionsGenerator
		subscriptions     []pulsar.Consumer
		wg                *sync.WaitGroup
		done              chan bool // on close the 'done' channel, all consumers jobs should close consumer and return.
		subscriptionNames []string  // contains subscriptions names, subscription name for pulsar driver need to be unique.

		ctxImporter hexa.ContextExporterImporter
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

func (r *receiver) Subscribe(name, channel string, payloadInstance interface{}, h hevent.EventHandler) error {
	if err := r.checkSubscriptionNameIsUnique(name); err != nil {
		return tracer.Trace(err)
	}

	if channel == "" {
		return tracer.Trace(errors.New("channel name can not be empty"))
	}

	consumer, err := r.consumer(hevent.NewChannelNames(name, channel))
	if err != nil {
		return tracer.Trace(err)
	}

	return r.subscribe(consumer, payloadInstance, h)
}

func (r *receiver) subscribe(consumer pulsar.Consumer, pi interface{}, h hevent.EventHandler) error {

	r.wg.Add(1)
	go receive(consumer, r.wg, r.done, func(msg pulsar.ConsumerMessage) {
		ctx, message, err := r.extractMessage(msg, pi)
		h(newHandlerCtx(msg), ctx, message, err)
	})
	return nil
}

func (r *receiver) SubscribeMulti(channels hevent.ChannelNames, payloadInstance interface{}, h hevent.EventHandler) error {
	if err := r.checkSubscriptionNameIsUnique(channels.SubscriptionName); err != nil {
		return tracer.Trace(err)
	}

	if err := channels.Validate(); err != nil {
		return tracer.Trace(err)
	}

	consumer, err := r.consumer(channels)
	if err != nil {
		return tracer.Trace(err)
	}

	return r.subscribe(consumer, payloadInstance, h)
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
func (r *receiver) checkSubscriptionNameIsUnique(name string) error {
	if gutil.Contains(r.subscriptionNames, name) {
		return tracer.Trace(errors.New("name is not unique"))
	}

	r.subscriptionNames = append(r.subscriptionNames, name)
	return nil
}

// consumer returns new instance of the pulsar consumer with provided channel
func (r *receiver) consumer(topics hevent.ChannelNames) (pulsar.Consumer, error) {
	options, err := r.cg(r.client, topics)
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
	ctx, err = r.ctxImporter.Import(rawMsg.MessageHeader.Ctx)
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
func NewReceiver(client pulsar.Client, options ReceiverOptions) (hevent.Receiver, error) {
	if client == nil {
		return nil, tracer.Trace(errors.New("client can not be nil"))
	}

	return &receiver{
		ctxImporter:   options.CtxImporter,
		client:        client,
		cg:            options.ConsumerOptionsGenerator,
		subscriptions: make([]pulsar.Consumer, 0),
		wg:            &sync.WaitGroup{},
		done:          make(chan bool),
	}, nil
}

var _ hevent.HandlerContext = &handlerContext{}
var _ hevent.Receiver = &receiver{}
