package kafkabox

import (
	"encoding/json"
	"time"

	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
)

type MessageConverter interface {
	EventToOutboxMessage(hexa.Context, *hevent.Event) (*OutboxMessage, error)
}

type messageConverter struct {
	rawMsgConverter hevent.RawMessageConverter
}

func newMessageConverter(c hevent.RawMessageConverter) MessageConverter {
	return &messageConverter{
		rawMsgConverter: c,
	}
}

func (c *messageConverter) EventToOutboxMessage(ctx hexa.Context, event *hevent.Event) (*OutboxMessage, error) {
	raw, err := c.rawMsgConverter.EventToRaw(ctx, event)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	// Move headers from raw message to the native kafka message headers.
	headers := c.headers(raw)
	raw.Headers = nil

	val, err := json.Marshal(raw)
	if err != nil {
		return nil, tracer.Trace(err)
	}

	return &OutboxMessage{
		ID:        gutil.UUID(),
		Topic:     event.Channel,
		Key:       event.Key,
		Value:     string(val),
		Headers:   headers,
		CreatedAt: time.Now(),
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
