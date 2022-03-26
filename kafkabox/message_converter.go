package kafkabox

import (
	"context"
	"time"

	"github.com/kamva/gutil"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
)

type MessageConverter interface {
	EventToOutboxMessage(context.Context, *hevent.Event) (*OutboxMessage, error)
}

type messageConverter struct {
	rawMsgConverter hevent.RawMessageConverter
}

func newMessageConverter(c hevent.RawMessageConverter) MessageConverter {
	return &messageConverter{
		rawMsgConverter: c,
	}
}

func (c *messageConverter) EventToOutboxMessage(ctx context.Context, event *hevent.Event) (*OutboxMessage, error) {
	raw, err := c.rawMsgConverter.EventToRaw(ctx, event)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	// Currently the RawMessage type has two `Headers` and `Payload` fields, we store each field
	// as an outbox model's field in our DB. If later RawMessage added another field, we can add
	// extra fields as a header.

	return &OutboxMessage{
		ID:        gutil.UUID(),
		Topic:     event.Channel,
		Key:       event.Key,
		Value:     string(raw.Payload),
		Headers:   c.headers(raw),
		EmittedAt: time.Now(),
	}, nil
}

// headers converts hevent raw headers to sarama message headers.
func (c *messageConverter) headers(raw *hevent.RawMessage) []Header {
	headers := make([]Header, len(raw.Headers))
	i := 0
	for k, v := range raw.Headers {
		headers[i] = Header{
			Key:   k,
			Value: string(v),
		}
		i++
	}
	return headers
}

var _ MessageConverter = &messageConverter{}
