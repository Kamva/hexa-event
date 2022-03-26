package main

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa-event/examples/kafka_dead_letter_queue/events"
	"github.com/kamva/hexa-event/hafka"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
)

const (
	Version = "2.3.0"
)

var BootstrapServers = []string{"localhost:9092"}
var l = hlog.NewPrinterDriver(hlog.DebugLevel)
var t = hexatranslator.NewEmptyDriver()
var p = hexa.NewContextPropagator(l, t)
var version = gutil.Must(sarama.ParseKafkaVersion(Version)).(sarama.KafkaVersion)

func main() {
	cfg := hafka.NewConfig(
		hafka.WithVersion(version),
		hafka.WithInitialOffset(sarama.OffsetOldest),
	)

	client := gutil.Must(sarama.NewClient(BootstrapServers, cfg)).(sarama.Client)
	emitter, err := hafka.NewEmitter(hafka.EmitterOptions{
		Client:            client,
		ContextPropagator: p,
		Encoder:           hevent.NewProtobufEncoder(),
	})

	defer emitter.Shutdown(context.Background())

	receiver, err := hafka.NewReceiver(hafka.ReceiverOptions{
		ContextPropagator: p,
		Client:            client,
		Middlewares:       []hevent.Middleware{hevent.RecoverMiddleware},
	})

	gutil.PanicErr(err)
	defer receiver.Shutdown(context.Background())

	gutil.PanicErr(err)

	sendEvent(emitter, "war")
	subscribeToEvents(receiver)

	gutil.PanicErr(receiver.Run()) // receiver start non-blocking

	gutil.WaitForSignals(syscall.SIGINFO, syscall.SIGTERM)
}

func sendEvent(e hevent.Emitter, topic string) {
	hctx := hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId:  "war_correlation_id",
		Locale:         "en-US",
		User:           hexa.NewGuest(),
		BaseLogger:     l,
		BaseTranslator: t,
	})

	_, err := e.Emit(hctx, &hevent.Event{
		Key:     "war_key",
		Channel: topic,
		Payload: &events.EventPayloadHello{
			Name: "ali",
		},
	})

	hlog.Debug("sent one message")
	gutil.PanicErr(err)
}

func subscribeToEvents(receiver hevent.Receiver) {
	cfg := hafka.NewConfig(
		hafka.WithVersion(version),
		hafka.WithInitialOffset(sarama.OffsetOldest),
	)
	err := receiver.SubscribeWithOptions(hafka.NewSubscriptionOptions(hafka.ConsumerOptions{
		BootstrapServers: BootstrapServers,
		Config:           cfg,
		Topic:            "war",
		RetryTopic:       "check_war_message",
		Group:            "check_war_message",
		RetryPolicy: hafka.RetryPolicy{
			InitialInterval:    time.Second * 10,
			BackoffCoefficient: 2,
			MaximumAttempts:    4,
		},
		Handler: helloHandler,
	}))
	gutil.PanicErr(err)
}

func helloHandler(c hevent.HandlerContext, msg hevent.Message, err error) error {
	gutil.PanicErr(err)
	var p events.EventPayloadHello
	gutil.PanicErr(msg.Payload.Decode(&p))

	hlog.CtxLogger(c).Info("ctx correlation_id", hlog.String("cid", hexa.CtxCorrelationId(c)))
	hlog.CtxLogger(c).Info(fmt.Sprintf("hi %s", p.Name))

	return errors.New("fake error just to retry the event")
}
