package hexapulsar

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

type (

	// receiver is implementation of the event receiver.
	receiver struct {
		client                   pulsar.Client
		consumerOptionsGenerator consumerOptionsGenerator
		consumers                []pulsar.Consumer
		wg                       *sync.WaitGroup
		done                     chan bool // on close the 'done' channel, all consumers jobs should close consumer and return.

		p            hexa.ContextPropagator
		encoder      hevent.Encoder
		msgConverter hevent.RawMessageConverter
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
func (r *receiver) Subscribe(channel string, h hevent.EventHandler) error {
	consumer, err := r.consumer(hevent.NewSubscriptionOptions(channel, h))
	if err != nil {
		return tracer.Trace(err)
	}

	return r.subscribe(consumer, h)
}

func (r *receiver) SubscribeWithOptions(so *hevent.SubscriptionOptions) error {
	if err := so.Validate(); err != nil {
		return tracer.Trace(err)
	}

	consumer, err := r.consumer(so)
	if err != nil {
		return tracer.Trace(err)
	}

	return r.subscribe(consumer, so.Handler)
}

func (r *receiver) Run() error {
	r.wg.Wait()
	return nil
}

func (r *receiver) Shutdown(_ context.Context) error {
	close(r.done)
	r.wg.Wait()
	return nil
}

func (r *receiver) subscribe(consumer pulsar.Consumer, h hevent.EventHandler) error {
	r.wg.Add(1)
	go receive(consumer, r.wg, r.done, func(msg pulsar.ConsumerMessage) {
		ctx, message, err := r.extractMessage(msg)
		// Note: we just log the handler error, if you want to send nack,
		// you should call to the Nack method.
		if err := h(newHandlerCtx(msg), ctx, message, err); err != nil {
			ctx.Logger().Error("error on handling event",
				hlog.String("topic", msg.Topic()),
				hlog.String("key", msg.Key()),
				hlog.String("subscription", msg.Subscription()),
				hlog.Any("headers", message.Headers),
				hlog.Err(err),
				hlog.ErrStack(err),
			)
		}
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

func (r *receiver) extractMessage(msg pulsar.ConsumerMessage) (ctx hexa.Context, m hevent.Message, err error) {
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

	ctx, m, err = r.msgConverter.RawMsgToMessage(context.Background(), &rawMsg)
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

type ReceiverOptions struct {
	Client            pulsar.Client
	ContextPropagator hexa.ContextPropagator
}

// NewReceiver returns new instance of pulsar implementation of the hexa event receiver.
func NewReceiver(o ReceiverOptions) (hevent.Receiver, error) {
	if o.Client == nil {
		return nil, tracer.Trace(errors.New("client can not be nil"))
	}

	return &receiver{
		p:                        o.ContextPropagator,
		client:                   o.Client,
		consumerOptionsGenerator: consumerOptionsGenerator{},
		consumers:                make([]pulsar.Consumer, 0),
		wg:                       &sync.WaitGroup{},
		done:                     make(chan bool),
		msgConverter:             hevent.NewRawMessageConverter(o.ContextPropagator, nil),
	}, nil
}

var _ hevent.HandlerContext = &handlerContext{}
var _ hevent.Receiver = &receiver{}
