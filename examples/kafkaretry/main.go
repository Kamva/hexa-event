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
	"github.com/kamva/hexa-event/hafka"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
)

const Version = "2.3.0"

var BootstrapServers = []string{"localhost:9092"}
var l = hlog.NewPrinterDriver(hlog.DebugLevel)
var t = hexatranslator.NewEmptyDriver()
var p = hexa.NewContextPropagator(l, t)
var version = gutil.Must(sarama.ParseKafkaVersion(Version)).(sarama.KafkaVersion)

type HelloPayload struct {
	Name string `json:"name"`
}

func main() {
	cfg := hafka.NewConfig(
		hafka.WithVersion(version),
		hafka.WithInitialOffset(sarama.OffsetOldest),
	)

	client := gutil.Must(sarama.NewClient(BootstrapServers, cfg)).(sarama.Client)
	emitter, err := hafka.NewEmitter(hafka.EmitterOptions{
		Client:            client,
		ContextPropagator: p,
		Encoder:           hevent.NewJsonEncoder(),
	})
	defer emitter.Shutdown(context.Background())

	receiver, err := hafka.NewReceiver(hafka.ReceiverOptions{
		ContextPropagator: p,
		Client:            client,
	})
	gutil.PanicErr(err)
	defer receiver.Shutdown(context.Background())

	sendEvent(emitter, "salam")
	subscribeToEvents(receiver)

	gutil.PanicErr(receiver.Run()) // receiver start non-blocking

	gutil.WaitForSignals(syscall.SIGINFO, syscall.SIGTERM)
}

func sendEvent(e hevent.Emitter, topic string) {
	hctx := hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId: "salam_correlation_id",
		Locale:        "en-US",
		User:          hexa.NewGuest(),
		Logger:        l,
		Translator:    t,
	})

	_, err := e.Emit(hctx, &hevent.Event{
		Key:     "salam_key",
		Channel: topic,
		Payload: &HelloPayload{
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
		Topic:            "salam",
		RetryTopic:       "check_salam_message",
		Group:            "check_salam_message",
		RetryPolicy: hafka.RetryPolicy{
			InitialInterval:    time.Second * 10,
			BackoffCoefficient: 2,
			MaximumAttempts:    4,
		},
		Handler:         helloHandler,
		PayloadInstance: &HelloPayload{},
	}))
	gutil.PanicErr(err)
}

var retryCount = -1

func helloHandler(hc hevent.HandlerContext, c hexa.Context, msg hevent.Message, err error) error {
	gutil.PanicErr(err)

	c.Logger().Info("ctx correlation_id", hlog.String("cid", c.CorrelationID()))
	c.Logger().Info(fmt.Sprintf("hi %s", msg.Payload.(*HelloPayload).Name))

	retryCount++
	if retryCount <= 3 {
		return errors.New("fake error just to retry the event")
	}
	c.Logger().Info("ok, I'm processed this message, by :)")
	return nil
}
