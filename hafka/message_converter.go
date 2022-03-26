package hafka

import (
	"context"

	"github.com/Shopify/sarama"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
)

type MessageConverter interface {
	EventToProducerMessage(context.Context, *hevent.Event) (*sarama.ProducerMessage, error)
	ConsumerMessageToEventMessage(msg *sarama.ConsumerMessage) (context.Context, hevent.Message, error)
	ConsumerToProducerMessage(newTopic string, msg *sarama.ConsumerMessage) *sarama.ProducerMessage
}

type messageConverter struct {
	rawMsgConverter hevent.RawMessageConverter
}

func newMessageConverter(c hevent.RawMessageConverter) MessageConverter {
	return &messageConverter{
		rawMsgConverter: c,
	}
}

func (c *messageConverter) EventToProducerMessage(ctx context.Context, event *hevent.Event) (*sarama.ProducerMessage, error) {
	raw, err := c.rawMsgConverter.EventToRaw(ctx, event)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	// Currently we Just put the raw message's headers and payload in our events, if later raw message
	// had another fields we should put the new fields in header before emitting the event.

	return &sarama.ProducerMessage{
		Topic:   event.Channel,
		Key:     sarama.StringEncoder(event.Key),
		Value:   sarama.ByteEncoder(raw.Payload),
		Headers: c.KafkaMessageHeaders(raw),
	}, nil
}

func (c *messageConverter) ConsumerMessageToEventMessage(msg *sarama.ConsumerMessage) (ctx context.Context, emsg hevent.Message, err error) {
	rawMsg := hevent.RawMessage{
		Headers: c.RawMessageHeaders(msg.Headers),
		Payload: msg.Value,
	}

	// validate the message
	if err = rawMsg.Validate(); err != nil {
		err = tracer.Trace(err)
		return
	}

	return c.rawMsgConverter.RawMsgToMessage(context.Background(), &rawMsg, msg)
}

func (c *messageConverter) ConsumerToProducerMessage(newTopic string, msg *sarama.ConsumerMessage) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic:   newTopic,
		Key:     sarama.ByteEncoder(msg.Key),
		Value:   sarama.ByteEncoder(msg.Value),
		Headers: c.KafkaMessageHeadersFromConsumerHeaders(msg.Headers),
	}
}

// KafkaMessageHeaders converts hevent raw headers to sarama message headers.
func (c *messageConverter) KafkaMessageHeaders(raw *hevent.RawMessage) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, len(raw.Headers))
	i := 0
	for k, v := range raw.Headers {
		headers[i] = sarama.RecordHeader{
			Key:   []byte(k),
			Value: v,
		}
		i++
	}
	return headers
}

// KafkaMessageHeadersFromConsumerHeaders converts received kafka message's headers to sarama
//  message headers needed by producer.
func (c *messageConverter) KafkaMessageHeadersFromConsumerHeaders(l []*sarama.RecordHeader) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, len(l))
	for i, v := range l {
		headers[i] = *v
	}
	return headers
}

func (c *messageConverter) RawMessageHeaders(recordHeaders []*sarama.RecordHeader) map[string][]byte {
	headers := make(map[string][]byte)
	for _, v := range recordHeaders {
		headers[string(v.Key)] = v.Value
	}
	return headers
}

var _ MessageConverter = &messageConverter{}
