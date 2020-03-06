package kpulsar

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Kamva/kitty"
	"github.com/Kamva/tracer"
	kevent "github.com/Kavma/kitty-event"
	"github.com/apache/pulsar-client-go/pulsar"
	"reflect"
	"sync"
)

type (
	SubscriptionItem struct {
		kevent.ChannelNames
		kevent.EventHandler
		pulsar.ConsumerOptions
	}

	// ConsumerOptionsGenerator generate new consumers.
	ConsumerOptionsGenerator func(pulsar.Client, kevent.ChannelNames) (pulsar.ConsumerOptions, error)

	// receiver is implementation of the event receiver. 
	receiver struct {
		client        pulsar.Client
		cg            ConsumerOptionsGenerator
		subscriptions []pulsar.Consumer
		wg            *sync.WaitGroup
		done          chan bool // on close the 'done' channel, all consumers jobs should close consumer and return.

		// Need these to create user context.
		uf kitty.UserFinder
		l  kitty.Logger
		t  kitty.Translator
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

func (r *receiver) Subscribe(channel string, h kevent.EventHandler) error {
	if channel == "" {
		return tracer.Trace(errors.New("channel name can not be empty"))
	}

	consumer, err := r.consumer(kevent.NewChannelNames([]string{channel}, ""))
	if err != nil {
		return tracer.Trace(err)
	}

	return r.subscribe(consumer, h)
}

func (r *receiver) subscribe(consumer pulsar.Consumer, h kevent.EventHandler) error {
	r.wg.Add(1)
	go receive(consumer, r.wg, r.done, func(msg pulsar.ConsumerMessage) {
		ctx, message, err := r.extractMessage(msg)
		h(newHandlerCtx(msg), ctx, message, err)
	})
	return nil
}

func (r *receiver) SubscribeMulti(channels kevent.ChannelNames, h kevent.EventHandler) error {
	if err := channels.Validate(); err != nil {
		return tracer.Trace(err)
	}

	consumer, err := r.consumer(channels)
	if err != nil {
		return tracer.Trace(err)
	}

	return r.subscribe(consumer, h)
}

func (r *receiver) Close() error {
	close(r.done)
	r.wg.Wait()
	return nil
}

// consumer returns new instance of the pulsar consumer with provided channel
func (r *receiver) consumer(topics kevent.ChannelNames) (pulsar.Consumer, error) {
	options, err := r.cg(r.client, topics)
	if err != nil {
		return nil, tracer.Trace(err)
	}
	return r.client.Subscribe(options)
}

func (r *receiver) extractMessage(msg pulsar.ConsumerMessage) (ctx kitty.Context, m kevent.Message, err error) {
	err = json.Unmarshal(msg.Message.Payload(), &m)
	if err != nil {
		err = tracer.Trace(err)
		return
	}

	// validate the message
	if err = m.Validate(); err != nil {
		err = tracer.Trace(err)
		return
	}

	// extract Context:
	ctx, err = kitty.CtxFromMap(m.Ctx, r.uf, r.l, r.t)
	if err != nil {
		err = tracer.Trace(err)
		return
	}

	return
}

// receive function receive message of a consume subscription.
func receive(consumer pulsar.Consumer, wg *sync.WaitGroup, done chan bool, f func(message pulsar.ConsumerMessage)) {
	defer wg.Done()

	ch := consumer.Chan()
	select {
	case msg := <-ch:
		f(msg)
	case _ = <-done:
		consumer.Close()
		return
	}
}

// newHandlerCtx returns new instance of the handler context.
func newHandlerCtx(msg pulsar.ConsumerMessage) kevent.HandlerContext {
	return &handlerContext{
		Context: context.Background(),
		msg:     msg,
	}
}

// ConsumerOptionsGeneratorByList get list of channels with their
// consumer options and return a consumer generator.
func ConsumerOptionsGeneratorByList(items []SubscriptionItem) ConsumerOptionsGenerator {
	return func(client pulsar.Client, channels kevent.ChannelNames) (options pulsar.ConsumerOptions, err error) {
		item := findSubscriptionItem(items, channels)
		if item == nil {
			err = tracer.Trace(errors.New("can not find topic options to generate new consumer"))
			return
		}
		options = item.ConsumerOptions
		return
	}
}

// findSubscriptionItem find subscription item in provided list.
func findSubscriptionItem(items []SubscriptionItem, channels kevent.ChannelNames) *SubscriptionItem {
	for _, item := range items {
		if reflect.DeepEqual(item.ChannelNames, channels) {
			return &item
		}
	}

	return nil
}

// NewReceiver returns new instance of pulsar implementation of the kitty event receiver.
func NewReceiver(client pulsar.Client, uf kitty.UserFinder, cg ConsumerOptionsGenerator) (kevent.Receiver, error) {
	if client == nil {
		return nil, tracer.Trace(errors.New("client can not be nil"))
	}

	return &receiver{
		uf:            uf,
		client:        client,
		cg:            cg,
		subscriptions: make([]pulsar.Consumer, 0),
		wg:            &sync.WaitGroup{},
		done:          make(chan bool),
	}, nil
}

var _ kevent.HandlerContext = &handlerContext{}
var _ kevent.Receiver = &receiver{}
