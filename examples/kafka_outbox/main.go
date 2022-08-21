package main

import (
	"context"

	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa-event/examples/kafka_outbox/events"
	"github.com/kamva/hexa-event/kafkabox"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/proto"
)

var l = hlog.NewPrinterDriver(hlog.DebugLevel)
var t = hexatranslator.NewEmptyDriver()
var p = hexa.NewContextPropagator(l, t)

func main() {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://root:12345@localhost:27017/?authSource=admin"))
	gutil.PanicErr(err)
	gutil.PanicErr(client.Connect(context.Background()))

	coll := client.Database("kafka_lab").Collection(kafkabox.CollectionName)

	emitter, err := kafkabox.NewEmitter(kafkabox.EmitterOptions{
		Outbox:            kafkabox.NewOutboxStore(coll),
		ContextPropagator: p,
		//Encoder:           hevent.NewProtobufEncoder(),
		Encoder:           hevent.NewProtobufEncoder(),
	})
	gutil.PanicErr(err)
	defer emitter.Shutdown(context.Background())

	gutil.PanicErr(sendEvent(emitter))

	hlog.Info("message sent, check your outbox.")
}

func sendEvent(emitter hevent.Emitter) error {
	m := events.EventPayloadHi{
		Name:   "ali",
		Age:    2000,
		Family: "rezai",
	}
	var _ proto.Message = &m

	var mi proto.Message = &m
	_ = mi
	hctx := hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId: "my_correlation_id",
		Locale:        "en-US",
		User:          hexa.NewGuest(),
		BaseLogger:        l,
		BaseTranslator:    t,
	})

	_, err := emitter.Emit(hctx, &hevent.Event{
		Key:     "hi_key",
		Channel: "hi",
		Payload: &events.EventPayloadHi{
			Name:   "ali",
			Age:    2000,
			Family: "rezai",
		},
	})

	return tracer.Trace(err)
}
