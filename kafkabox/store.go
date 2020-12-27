package kafkabox

import (
	"context"

	"github.com/kamva/hexa/db/mgmadapter"
	"github.com/kamva/tracer"
	"go.mongodb.org/mongo-driver/mongo"
)

type OutboxStore interface {
	Migrate() error // do migration if needed.
	Create(c context.Context, msg *OutboxMessage) error
	Close() error
}

type outboxStore struct {
	mgmadapter.Repository
	coll *mongo.Collection
}

func (s *outboxStore) Migrate() error {
	// We don't have anything for mongodb to migrate.
	return nil
}

func (s *outboxStore) Create(c context.Context, msg *OutboxMessage) error {
	_, err := s.coll.InsertOne(c, msg)
	return tracer.Trace(err)
}

func (s *outboxStore) Close() error {
	// we Don't need to do anything, connection should be closed by the app.
	return nil
}

func NewOutboxStore(coll *mongo.Collection) OutboxStore {
	return &outboxStore{coll: coll}
}