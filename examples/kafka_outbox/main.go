package main

import (
	"context"

	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa-event/kafkabox"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var l = hlog.NewPrinterDriver(hlog.DebugLevel)
var t = hexatranslator.NewEmptyDriver()
var p = hexa.NewContextPropagator(l, t)

type HelloPayload struct {
	Name string `json:"name"`
}

func main() {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://root:12345@localhost:27017/?authSource=admin"))
	gutil.PanicErr(err)
	gutil.PanicErr(client.Connect(context.Background()))

	coll := client.Database("kafka_lab").Collection("outbox")

	emitter, err := kafkabox.NewEmitter(kafkabox.EmitterOptions{
		Outbox:            kafkabox.NewOutboxStore(coll),
		ContextPropagator: p,
		Encoder:           hevent.NewJsonEncoder(),
	})
	gutil.PanicErr(err)
	defer emitter.Close()

	gutil.PanicErr(sendEvent(emitter))

	hlog.Info("message sent, check your outbox.")
}

func sendEvent(emitter hevent.Emitter) error {
	hctx := hexa.NewContext(hexa.ContextParams{
		CorrelationId: "my_correlation_id",
		Locale:        "en-US",
		User:          hexa.NewGuest(),
		Logger:        l,
		Translator:    t,
	})

	_, err := emitter.Emit(hctx, &hevent.Event{
		Key:     "hi_key",
		Channel: "hi",
		Payload: &HelloPayload{
			Name: "ali",
		},
	})

	return tracer.Trace(err)
}
