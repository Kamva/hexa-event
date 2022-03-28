package hafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

type ConsumerGroup interface {
	Consume() error
	Close() error
}

type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	Ready() <-chan struct{}
}

// ConsumerGroup will create sarama consumer group and consume it.
type consumerGroup struct {
	o         ConsumerOptions
	scg       sarama.ConsumerGroup
	cgHandler ConsumerGroupHandler
	qm        QueueManager
	wg        *sync.WaitGroup
	cancel    context.CancelFunc
}

func newConsumerGroup(o ConsumerOptions, qm QueueManager, scg sarama.ConsumerGroup, h ConsumerGroupHandler) ConsumerGroup {
	return &consumerGroup{
		o:         o,
		scg:       scg,
		cgHandler: h,
		wg:        &sync.WaitGroup{},
		qm:        qm,
	}
}

func (cg *consumerGroup) Consume() error {
	if cg.cancel != nil {
		return tracer.Trace(errors.New("you can not call to consume twice"))
	}

	hlog.Info("listening to the topic", append(retryPolicyLogFields(cg.o.RetryPolicy),
		hlog.String("group", cg.o.Group),
		hlog.String("topic", cg.o.Topic),
	)...)

	// Listen to the topic and its retry topics.
	topics := []string{cg.o.Topic}
	topics = append(topics, cg.qm.RetryTopics()...)
	ctx, cancel := context.WithCancel(context.Background())
	cg.cancel = cancel

	cg.wg.Add(1)
	go func() {
		defer cg.wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := cg.scg.Consume(ctx, topics, cg.cgHandler); err != nil {
				hlog.Error("error from kafka consumer", hlog.Err(err), hlog.ErrStack(tracer.Trace(err)))
				time.Sleep(30 * time.Second) // Wait 30 second before next try
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}

			hlog.Info("rebalanced Kafka consumer group", hlog.String("group", cg.o.Group))

			// The consumerGroupHandler's "ready" channel reset by CleanUp() method on the
			// consumerGroupHandler.
		}

	}()

	// Await till the consumer has been set up
	<-cg.cgHandler.Ready()
	return nil
}

func (cg *consumerGroup) Close() error {
	cg.cancel()
	cg.wg.Wait()
	return tracer.Trace(cg.scg.Close())
}

// cgHandler implements the sarma ConsumerGroupHandler
// Why we do not use single cgHandler for all of topics in the microservice?
// sometimes we need to register two or more handlers for one Topic, but in
// a consumer group we can not set two offset positions for one Topic, so we need
// to create multiple consumer groups to handle it.
// in addition to that if we use single consumer group, we can not detect what operation.
type cgHandler struct {
	o            ConsumerOptions
	handler      hevent.EventHandler
	qm           QueueManager
	msgConverter MessageConverter
	producer     sarama.AsyncProducer
	ready        chan struct{}
}

type ConsumerGroupHandlerOptions struct {
	ConsumerOptions  *ConsumerOptions
	QueueManager     QueueManager
	Handler          hevent.EventHandler
	MessageConverter MessageConverter
	Producer         sarama.AsyncProducer
}

func newConsumerGroupHandler(o ConsumerGroupHandlerOptions) ConsumerGroupHandler {

	return &cgHandler{
		o:            *o.ConsumerOptions,
		handler:      o.Handler,
		qm:           o.QueueManager,
		msgConverter: o.MessageConverter,
		producer:     o.Producer,
		ready:        make(chan struct{}),
	}
}

func (h *cgHandler) Setup(s sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *cgHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	h.ready = make(chan struct{})
	return nil
}

func (h *cgHandler) Ready() <-chan struct{} {
	return h.ready
}

func (h *cgHandler) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	retryCount, err := h.qm.RetryNumberFromTopic(claim.Topic())
	if err != nil {
		return tracer.Trace(err)
	}

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for msg := range claim.Messages() {
		hlog.Debug("received new kafka msg",
			hlog.String("groupId", h.o.Group),
			hlog.String("topic", msg.Topic),
			hlog.Int32("partition", msg.Partition),
			hlog.Time("time", msg.Timestamp),
			hlog.String("key", string(msg.Key)),
			hlog.String("value", string(msg.Value)),
			hlog.Int("retry_num", retryCount),
		)
		// backoff
		if retryCount != 0 {
			after := h.qm.RetryAfter(retryCount-1, msg.Timestamp)
			if after != 0 {
				time.Sleep(after)
			}
		}

		// Inject custom headers for message deduplication.
		hexaCtx, hmsg, err := h.msgConverter.ConsumerMessageToEventMessage(msg)
		hexaCtx = h.injectMessageDeduplicationMetaKeys(hexaCtx, msg)

		if err != nil {
			hlog.Error("can not convert raw message to hexa event message", h.logMsgErr(msg, err, retryCount)...)

			// retry the message
			// TODO: maybe we don't need to retry messages that we
			// can not convert to the hexa message.
			h.producer.Input() <- h.msgConverter.ConsumerToProducerMessage(h.qm.NextTopic(retryCount), msg)
		} else if err := h.handler(newNoopHandlerContext(hexaCtx), hmsg, err); err != nil {
			// log error
			hlog.CtxLogger(hexaCtx).Error("event handler failed to handle message", h.logMsgErr(msg, err, retryCount)...)

			// retry the message
			h.producer.Input() <- h.msgConverter.ConsumerToProducerMessage(h.qm.NextTopic(retryCount), msg)
		}

		s.MarkMessage(msg, "")
	}
	s.Commit()

	return nil
}

func (h *cgHandler) logMsgErr(msg *sarama.ConsumerMessage, err error, retryCount int) []hlog.Field {
	fields := []hlog.Field{
		hlog.String("topic", msg.Topic),
		hlog.String("key", string(msg.Key)),
		hlog.Int64("offset", msg.Offset),
		hlog.Int32("partition", msg.Partition),
		hlog.Int("retry_num", retryCount),
		hlog.String("next_topic", h.qm.NextTopic(retryCount)),
	}
	return append(fields, hlog.ErrFields(tracer.Trace(err))...)
}

// injectMessageDeduplicationMetaKeys injects message deduplication meta keys into the
// message headers for next retries and into the context to deduplicate messages.
func (h *cgHandler) injectMessageDeduplicationMetaKeys(ctx context.Context, msg *sarama.ConsumerMessage) context.Context {
	var rootEventId, rootActionName string

	eventId := generateEventId(msg.Topic, msg.Partition, msg.Offset)
	actionName := h.o.Group

	for _, h := range msg.Headers {
		if string(h.Key) == hevent.HexaRootEventID {
			rootEventId = string(h.Value)
		}

		if string(h.Key) == hevent.HexaRootEventHandlerActionName {
			rootActionName = string(h.Value)
		}
	}

	if rootEventId == "" {
		rootEventId = eventId
		msg.Headers = append(msg.Headers, &sarama.RecordHeader{
			Key:   []byte(hevent.HexaRootEventID),
			Value: []byte(rootEventId),
		})
	}

	if rootActionName == "" {
		rootActionName = actionName
		msg.Headers = append(msg.Headers, &sarama.RecordHeader{
			Key:   []byte(hevent.HexaRootEventHandlerActionName),
			Value: []byte(rootActionName),
		})
	}

	// Set all values into hexa context:
	ctx = context.WithValue(ctx, hevent.HexaEventID, eventId)
	ctx = context.WithValue(ctx, hevent.HexaEventHandlerActionName, actionName)
	ctx = context.WithValue(ctx, hevent.HexaRootEventID, rootEventId)
	ctx = context.WithValue(ctx, hevent.HexaRootEventHandlerActionName, rootActionName)
	return ctx
}

type noopHandlerContext struct {
	context.Context
}

func newNoopHandlerContext(ctx context.Context) hevent.HandlerContext {
	return &noopHandlerContext{ctx}
}

func (e *noopHandlerContext) Ack() {
	// Do nothing.
}

func (e *noopHandlerContext) Nack() {
	// Do nothing.
}

func generateEventId(topic string, partition int32, offset int64) string {
	return fmt.Sprintf("%s-%d-%d", topic, partition, offset)
}

var _ ConsumerGroupHandler = &cgHandler{}
var _ hevent.HandlerContext = &noopHandlerContext{}
