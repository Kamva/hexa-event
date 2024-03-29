package kafkabox

import (
	"context"

	"github.com/kamva/hexa"
	"github.com/kamva/hexa/db/mgmadapter"
	"github.com/kamva/tracer"
	"go.mongodb.org/mongo-driver/mongo"
)

const CollectionName = "outbox"

type OutboxStore interface {
	hexa.Shutdownable

	Migrate() error // do migration if needed.
	Create(c context.Context, msg *OutboxMessage) error
	Ping(c context.Context) error
}

type outboxStore struct {
	mgmadapter.Store
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

func (s *outboxStore) Ping(ctx context.Context) error {
	return tracer.Trace(s.coll.Database().Client().Ping(ctx, nil))
}

func (s *outboxStore) Shutdown(ctx context.Context) error {
	// We do not close anything, connection should be closed by the app.
	return nil
}

func NewOutboxStore(coll *mongo.Collection) OutboxStore {
	return &outboxStore{coll: coll}
}
