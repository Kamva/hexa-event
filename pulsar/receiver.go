package kpulsar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Kamva/gutil"
	"github.com/Kamva/kitty"
	"github.com/Kamva/kitty-event"
	"github.com/Kamva/tracer"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync"
)

type (
	SubscriptionItem struct {
		TopicNamingFormat string // We use this format to generate topics name.
		kevent.ChannelNames
		kevent.EventHandler
		pulsar.ConsumerOptions
	}

	// ConsumerOptionsGenerator generate new consumers.
	ConsumerOptionsGenerator func(client pulsar.Client, topic kevent.ChannelNames) (pulsar.ConsumerOptions, error)

	// consumerOptionsGenerator implements ConsumerOptionsGenerator function as its method.
	consumerOptionsGenerator struct {
		items []SubscriptionItem
	}

	// receiver is implementation of the event receiver.
	receiver struct {
		client            pulsar.Client
		cg                ConsumerOptionsGenerator
		subscriptions     []pulsar.Consumer
		wg                *sync.WaitGroup
		done              chan bool // on close the 'done' channel, all consumers jobs should close consumer and return.
		subscriptionNames []string  // contains subscriptions names, subscription name for pulsar driver need to be unique.

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

func (cg *consumerOptionsGenerator) Generator(client pulsar.Client, topics kevent.ChannelNames) (pulsar.ConsumerOptions, error) {
	item := cg.findSubscriptionItem(topics.SubscriptionName)
	if item == nil {
		err := tracer.Trace(fmt.Errorf("pulsar option for the topic %s not found", topics.SubscriptionName))
		return pulsar.ConsumerOptions{}, err
	}

	return cg.setTopicNameOnOptions(item.TopicNamingFormat, item.ConsumerOptions, topics), nil
}

// setTopicNameOnOptions set the topic name on the options.
func (cg *consumerOptionsGenerator) setTopicNameOnOptions(format string, options pulsar.ConsumerOptions, topics kevent.ChannelNames) pulsar.ConsumerOptions {
	options.Name = topics.SubscriptionName

	if len(topics.Names) == 1 {
		options.Topic = cg.setFinalTopicNames(format, topics.Names[0])[0]
		return options
	}

	if len(topics.Names) > 1 {
		options.Topics = cg.setFinalTopicNames(format, topics.Names...)
		return options
	}

	options.TopicsPattern = cg.setFinalTopicNames(format, topics.Pattern)[0]
	return options
}

// setFinalTopicNames set the final name of topics.
func (cg *consumerOptionsGenerator) setFinalTopicNames(format string, names ...string) []string {
	finalNames := make([]string, len(names))
	for i, n := range names {
		finalNames[i] = fmt.Sprintf(format, n)
	}

	return finalNames
}

// findSubscriptionItem find subscription item in provided list.
func (cg *consumerOptionsGenerator) findSubscriptionItem(subscriptionName string) *SubscriptionItem {
	for _, item := range cg.items {
		if item.ChannelNames.SubscriptionName == subscriptionName {
			return &item
		}
	}

	return nil
}

func (r *receiver) Subscribe(name, channel string, h kevent.EventHandler) error {
	if err := r.captureSubscriptionName(name); err != nil {
		return tracer.Trace(err)
	}

	if channel == "" {
		return tracer.Trace(errors.New("channel name can not be empty"))
	}

	consumer, err := r.consumer(kevent.NewChannelNames(name, channel))
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
	if err := r.captureSubscriptionName(channels.SubscriptionName); err != nil {
		return tracer.Trace(err)
	}

	if err := channels.Validate(); err != nil {
		return tracer.Trace(err)
	}

	consumer, err := r.consumer(channels)
	if err != nil {
		return tracer.Trace(err)
	}

	return r.subscribe(consumer, h)
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
func (r *receiver) captureSubscriptionName(name string) error {
	if gutil.Contains(r.subscriptionNames, name) {
		return tracer.Trace(errors.New("name is not unique"))
	}

	r.subscriptionNames = append(r.subscriptionNames, name)
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
func newHandlerCtx(msg pulsar.ConsumerMessage) kevent.HandlerContext {
	return &handlerContext{
		Context: context.Background(),
		msg:     msg,
	}
}

// ConsumerOptionsGeneratorByList get list of channels with their
// consumer options and return a consumer generator.
func NewConsumerOptionsGenerator(items []SubscriptionItem) ConsumerOptionsGenerator {
	g := &consumerOptionsGenerator{items: items}

	return g.Generator
}

// ConsumerOptions returns new instance of pulsar consumer options.
func ConsumerOptions(name string, subscriptionType pulsar.SubscriptionType) pulsar.ConsumerOptions {
	return pulsar.ConsumerOptions{
		SubscriptionName: name,
		Type:             subscriptionType,
	}
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
