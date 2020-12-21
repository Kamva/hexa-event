package hafka

import (
	"context"
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
)

type MessageConverter interface {
	EventToProducerMessage(hexa.Context, *hevent.Event) (*sarama.ProducerMessage, error)
	ConsumerMessageToEventMessage(msg *sarama.ConsumerMessage, payloadInstance interface{}) (hexa.Context, hevent.Message, error)
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

func (c *messageConverter) EventToProducerMessage(ctx hexa.Context, event *hevent.Event) (*sarama.ProducerMessage, error) {
	raw, err := c.rawMsgConverter.EventToRaw(ctx, event)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	// Move headers from raw message to the native kafka message headers.
	headers := c.KafkaMessageHeaders(raw)
	raw.Headers = nil

	val, err := json.Marshal(raw)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	return &sarama.ProducerMessage{
		Topic:   event.Channel,
		Key:     sarama.StringEncoder(event.Key),
		Value:   sarama.ByteEncoder(val),
		Headers: headers,
	}, nil
}

func (c *messageConverter) ConsumerMessageToEventMessage(msg *sarama.ConsumerMessage, payloadInstance interface{}) (ctx hexa.Context, emsg hevent.Message, err error) {
	rawMsg := hevent.RawMessage{}
	err = json.Unmarshal(msg.Value, &rawMsg)
	if err != nil {
		err = tracer.Trace(err)
		return
	}
	rawMsg.Headers = c.RawMessageHeaders(msg.Headers)

	// validate the message
	if err = rawMsg.Validate(); err != nil {
		err = tracer.Trace(err)
		return
	}

	return c.rawMsgConverter.RawMsgToMessage(context.Background(), &rawMsg, payloadInstance)
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

// KafkaMessageHeaders converts hevent raw headers to sarama message headers.
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
