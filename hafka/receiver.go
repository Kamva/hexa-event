package hafka

import (
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
)

type receiver struct {
	p              hexa.ContextPropagator
	cfg            *sarama.Config
	consumerGroups []ConsumerGroup
	producer       sarama.AsyncProducer
}

type ReceiverOptions struct {
	ContextPropagator hexa.ContextPropagator
	// This is the global config, for now we don't use it
	// and you should provide config per subscription.
	Config   *sarama.Config
	Producer sarama.AsyncProducer
}

func NewReceiver(o ReceiverOptions) hevent.Receiver {
	return &receiver{
		p:              o.ContextPropagator,
		cfg:            o.Config,
		consumerGroups: make([]ConsumerGroup, 0),
		producer:       o.Producer,
	}
}

func (r *receiver) Subscribe(channel string, payloadInstance interface{}, h hevent.EventHandler) error {
	return r.SubscribeWithOptions(&hevent.SubscriptionOptions{
		Channel:         channel,
		PayloadInstance: payloadInstance,
		Handler:         h,
	})
}

func (r *receiver) SubscribeWithOptions(options *hevent.SubscriptionOptions) error {
	if err := options.Validate(); err != nil {
		return tracer.Trace(err)
	}

	consumerOptions := extractConsumerOptions(options)
	if consumerOptions == nil {
		return tracer.Trace(errors.New("kafka consumer options can not be nil"))
	}

	// We do tno support channels and channel pattern.
	consumerOptions.Topic = options.Channel
	consumerOptions.PayloadInstance = options.PayloadInstance
	*consumerOptions = mergeWithDefaultConsumerOptions(*consumerOptions)

	if err := consumerOptions.Validate(); err != nil {
		return tracer.Trace(err)
	}

	qm := newQueueManager(consumerOptions.Topic, consumerOptions.RetryPolicy)
	cgh := newConsumerGroupHandler(ConsumerGroupHandlerOptions{
		ConsumerOptions:  consumerOptions,
		QueueManager:     qm,
		Handler:          options.Handler,
		MessageConverter: newMessageConverter(hevent.NewRawMessageConverter(r.p, hevent.NewProtobufEncoder())),
		Producer:         r.producer,
	})
	consumerGroup, err := sarama.NewConsumerGroup(
		consumerOptions.BootstrapServers,
		consumerOptions.Group,
		consumerOptions.Config,
	)
	if err != nil {
		return tracer.Trace(err)
	}

	cg := newConsumerGroup(*consumerOptions, qm, consumerGroup, cgh)
	r.consumerGroups = append(r.consumerGroups, cg)
	return nil
}

func (r *receiver) Start() error {
	wg := &sync.WaitGroup{}
	errs := make(chan error, len(r.consumerGroups))
	defer close(errs)

	for _, cg := range r.consumerGroups {
		wg.Add(1)
		go func(cg ConsumerGroup) {
			defer wg.Done()

			errs <- tracer.Trace(cg.Consume())
		}(cg)
	}

	// wait to run all consumers
	wg.Wait()
	// return first error:
	for i := 0; i < len(r.consumerGroups); i++ {
		if err := <-errs; err != nil {
			return tracer.Trace(err)
		}
	}

	return nil
}

func (r *receiver) Close() error {
	wg := &sync.WaitGroup{}
	errs := make(chan error, len(r.consumerGroups))
	defer close(errs)

	for _, cg := range r.consumerGroups {
		wg.Add(1)
		go func(cg ConsumerGroup) {
			wg.Done()
			errs <- cg.Close()
		}(cg)
	}

	// wait to close all consumerGroups.
	wg.Wait()

	for i := 0; i < len(r.consumerGroups); i++ {
		if err := <-errs; err != nil {
			return tracer.Trace(err)
		}
	}

	return nil
}

var _ hevent.Receiver = &receiver{}
