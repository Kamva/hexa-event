package hafka

import (
	"context"
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

// ConsumerGroup will create sarama consumer group and consume it.
type consumerGroup struct {
	o         ConsumerOptions
	scg       sarama.ConsumerGroup
	cgHandler sarama.ConsumerGroupHandler
	qm        QueueManager
}

func newConsumerGroup(o ConsumerOptions, qm QueueManager, scg sarama.ConsumerGroup, h sarama.ConsumerGroupHandler) ConsumerGroup {
	return &consumerGroup{
		o:         o,
		scg:       scg,
		cgHandler: h,
		qm:        qm,
	}
}

func (cg *consumerGroup) Consume() error {
	// Listen to the topic and its retry topics.
	topics := []string{cg.o.Topic}
	topics = append(topics, cg.qm.RetryTopics()...)

	return tracer.Trace(cg.scg.Consume(context.Background(), topics, cg.cgHandler))
}

func (cg *consumerGroup) Close() error {
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
}

type ConsumerGroupHandlerOptions struct {
	ConsumerOptions  *ConsumerOptions
	QueueManager     QueueManager
	Handler          hevent.EventHandler
	MessageConverter MessageConverter
	Producer         sarama.AsyncProducer
}

func newConsumerGroupHandler(o ConsumerGroupHandlerOptions) sarama.ConsumerGroupHandler {
	return &cgHandler{
		o:            *o.ConsumerOptions,
		handler:      o.Handler,
		qm:           o.QueueManager,
		msgConverter: o.MessageConverter,
		producer:     o.Producer,
	}
}

func (h *cgHandler) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (h *cgHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
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
		hlog.Debug("new kafka msg",
			hlog.String("groupId", h.o.Group),
			hlog.String("Topic", msg.Topic),
			hlog.Int32("partition", msg.Partition),
			hlog.Time("time", msg.Timestamp),
			hlog.String("key", string(msg.Key)),
			hlog.String("value", string(msg.Value)),
		)
		// backoff
		if retryCount != 0 {
			after := h.qm.RetryAfter(retryCount-1, msg.Timestamp)
			if after != 0 {
				time.Sleep(after)
			}
		}

		hctx, hmsg, err := h.msgConverter.ConsumerMessageToEventMessage(msg, h.o.PayloadInstance)

		if err := h.handler(newEmptyHandlerContext(), hctx, hmsg, err); err != nil {
			// log error
			hctx.Logger().Error("error on handling kafka event",
				hlog.String("topic", msg.Topic),
				hlog.String("key", string(msg.Key)),
				hlog.Int64("offset", msg.Offset),
				hlog.Int32("partition", msg.Partition),
				hlog.Any("headers", hmsg.Headers),
				hlog.Err(err),
				hlog.ErrStack(err),
			)

			// retry the message
			h.producer.Input() <- h.msgConverter.ConsumerToProducerMessage(h.qm.NextTopic(retryCount), msg)
		}

		s.MarkMessage(msg, "")
	}
	s.Commit()

	return nil
}

type emptyHandlerContext struct {
	context.Context
}

func newEmptyHandlerContext() hevent.HandlerContext {
	return &emptyHandlerContext{context.Background()}
}

func (e *emptyHandlerContext) Ack() {
	// Do nothing.
}

func (e *emptyHandlerContext) Nack() {
	// Do nothing.
}

var _ sarama.ConsumerGroupHandler = &cgHandler{}
var _ hevent.HandlerContext = &emptyHandlerContext{}
