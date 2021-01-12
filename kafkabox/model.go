package kafkabox

import "time"

// Header is the Kafka event's header
type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// OutboxMessage is the outbox collection's model
type OutboxMessage struct {
	ID        string    `bson:"id" json:"id"`
	Topic     string    `bson:"topic" json:"topic"`
	Key       string    `bson:"key" json:"key"`
	Value     string    `bson:"value" json:"value"`
	Headers   []Header  `bson:"headers" json:"headers"`
	EmittedAt time.Time `bson:"emitted_at" json:"emitted_at"`
}
