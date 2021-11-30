package hafka

import (
	"context"
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

type receiver struct {
	p              hexa.ContextPropagator
	consumerGroups []ConsumerGroup
	producerClient sarama.Client
	producer       sarama.AsyncProducer
	middlewares    []hevent.MiddlewareFunc // global middlewares
}

func (r *receiver) HealthIdentifier() string {
	return "kafka_receiver"
}

func (r *receiver) LivenessStatus(_ context.Context) hexa.LivenessStatus {
	if len(r.producerClient.Brokers()) > 0 {
		return hexa.StatusAlive
	}

	return hexa.StatusDead
}

func (r *receiver) ReadinessStatus(_ context.Context) hexa.ReadinessStatus {
	if len(r.producerClient.Brokers()) > 0 {
		return hexa.StatusReady
	}

	return hexa.StatusUnReady
}

func (r *receiver) HealthStatus(ctx context.Context) hexa.HealthStatus {
	return hexa.HealthStatus{
		Id:    r.HealthIdentifier(),
		Alive: r.LivenessStatus(ctx),
		Ready: r.ReadinessStatus(ctx),
	}
}

type ReceiverOptions struct {
	ContextPropagator hexa.ContextPropagator

	// We use this client to crate producer to push
	// messages retry queues.
	Client sarama.Client

	Middlewares []hevent.MiddlewareFunc
}

func NewReceiver(o ReceiverOptions) (hevent.Receiver, error) {
	producer, err := sarama.NewAsyncProducerFromClient(o.Client)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	return &receiver{
		p:              o.ContextPropagator,
		consumerGroups: make([]ConsumerGroup, 0),
		producerClient: o.Client,
		producer:       producer,
		middlewares:    o.Middlewares,
	}, nil
}

func (r *receiver) Subscribe(channel string, h hevent.EventHandler) error {
	return r.SubscribeWithOptions(&hevent.SubscriptionOptions{
		Channel: channel,
		Handler: h,
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
	*consumerOptions = mergeWithDefaultConsumerOptions(*consumerOptions)

	if err := consumerOptions.Validate(); err != nil {
		return tracer.Trace(err)
	}

	qm := newQueueManager(consumerOptions.RetryTopic, consumerOptions.RetryPolicy)
	cgh := newConsumerGroupHandler(ConsumerGroupHandlerOptions{
		ConsumerOptions:  consumerOptions,
		QueueManager:     qm,
		Handler:          hevent.WithMiddlewares(options.Handler, r.middlewares...),
		MessageConverter: newMessageConverter(hevent.NewRawMessageConverter(r.p, nil)),
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

func (r *receiver) Run() error {
	wg := &sync.WaitGroup{}
	errs := make(chan error, len(r.consumerGroups))
	defer close(errs)

	for _, cg := range r.consumerGroups {
		wg.Add(1)
		go func(cg ConsumerGroup) {
			defer wg.Done()

			// this function is non-blocking and return after initialization
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

	hlog.Info("Kafka consumer groups up and running...")

	return nil
}

func (r *receiver) Shutdown(_ context.Context) error {
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
var _ hexa.Health = &receiver{}
