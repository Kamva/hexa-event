package kafkabox

import "time"

// Header is the Kafka event's header
type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// OutboxMessage is the outbox model
type OutboxMessage struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Headers   []Header  `json:"headers"`
	CreatedAt time.Time `json:"created_at"`
}
