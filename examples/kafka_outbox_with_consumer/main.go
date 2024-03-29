package main

import (
	"context"
	"flag"
	"fmt"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa-event/examples/kafka_outbox_with_consumer/hello"
	"github.com/kamva/hexa-event/hafka"
	"github.com/kamva/hexa-event/kafkabox"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const Version = "2.3.0"
const channel = "salam"

var BootstrapServers = []string{"localhost:9092"}

const DB = "kafkalab"

var l = hlog.NewPrinterDriver(hlog.DebugLevel)
var t = hexatranslator.NewEmptyDriver()
var p = hexa.NewContextPropagator(l, t)
var version = gutil.Must(sarama.ParseKafkaVersion(Version)).(sarama.KafkaVersion)
var dbUrl = "mongodb+srv://root:12345@air.hjflq.gcp.mongodb.net/kafkalab?authSource=admin"

func main() {
	// Note: to run this example you need to run your kafka with Mongo outbox connector.
	// Use docker-compose in the mongo-outbox project.
	var mode string
	flag.StringVar(&mode, "mode", "emit", "mode can be receive or emit")
	flag.Parse()

	if mode != "emit" && mode != "receive" {
		l.Error("invalid mode")
		return
	}
	if mode == "emit" {
		emit()
		return
	}

	receive()
	return
}

func emit() {
	client, err := mongo.NewClient(options.Client().ApplyURI(dbUrl))
	gutil.PanicErr(err)
	gutil.PanicErr(client.Connect(context.Background()))

	coll := client.Database(DB).Collection(kafkabox.CollectionName)

	emitter, err := kafkabox.NewEmitter(kafkabox.EmitterOptions{
		Outbox:            kafkabox.NewOutboxStore(coll),
		ContextPropagator: p,
		Encoder:           hevent.NewProtobufEncoder(),
	})
	gutil.PanicErr(err)
	defer emitter.Shutdown(context.Background())

	gutil.PanicErr(sendEvent(emitter))

	hlog.Info("message sent, check your outbox.")
}

func sendEvent(emitter hevent.Emitter) error {
	hctx := hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId:  "my_correlation_id",
		Locale:         "",
		User:           hexa.NewGuest(),
		BaseLogger:     l,
		BaseTranslator: t,
	})

	_, err := emitter.Emit(hctx, &hevent.Event{
		Key:     "hi_key",
		Channel: channel,
		Payload: &hello.HelloPayload{
			Name: "reza",
			Age:  42,
		},
	})

	return tracer.Trace(err)
}

func receive() {
	cfg := hafka.NewConfig(
		hafka.WithVersion(version),
		hafka.WithInitialOffset(sarama.OffsetOldest),
	)

	client := gutil.Must(sarama.NewClient(BootstrapServers, cfg)).(sarama.Client)
	receiver, err := hafka.NewReceiver(hafka.ReceiverOptions{
		ContextPropagator: p,
		Client:            client,
		Middlewares:       []hevent.Middleware{hevent.RecoverMiddleware},
	})
	gutil.PanicErr(err)
	defer receiver.Shutdown(context.Background())

	subscribeToEvents(receiver)

	gutil.PanicErr(receiver.Run()) // receiver start non-blocking

	gutil.WaitForSignals(syscall.SIGINFO, syscall.SIGTERM)
}

func subscribeToEvents(receiver hevent.Receiver) {
	cfg := hafka.NewConfig(
		hafka.WithVersion(version),
		hafka.WithInitialOffset(sarama.OffsetOldest),
	)
	err := receiver.SubscribeWithOptions(hafka.NewSubscriptionOptions(hafka.ConsumerOptions{
		BootstrapServers: BootstrapServers,
		Config:           cfg,
		Topic:            channel,
		RetryTopic:       "check_salam_message",
		Group:            "check_salam_message",
		RetryPolicy: hafka.RetryPolicy{
			InitialInterval:    time.Second * 10,
			BackoffCoefficient: 2,
			MaximumAttempts:    3,
		},
		Handler: helloHandler,
	}))
	gutil.PanicErr(err)
}

var retryCount = -1

func helloHandler(c hevent.HandlerContext, msg hevent.Message, err error) error {
	gutil.PanicErr(err)

	var p hello.HelloPayload
	gutil.PanicErr(msg.Payload.Decode(&p))

	l := hexa.Logger(c)
	l.Info("ctx correlation_id", hlog.String("cid", hexa.CtxCorrelationId(c)))
	l.Info(fmt.Sprintf("hi %s", p.Name))

	l.Info("ok, I'm processed this message, by :)")
	return nil
}
