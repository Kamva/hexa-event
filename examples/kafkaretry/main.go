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
		Middlewares:       []hevent.Middleware{hevent.RecoverMiddleware},
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
		CorrelationId:  "salam_correlation_id",
		Locale:         "en-US",
		User:           hexa.NewGuest(),
		BaseLogger:     l,
		BaseTranslator: t,
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
		Handler: helloHandler,
	}))
	gutil.PanicErr(err)
}

var retryCount = -1

func helloHandler(c hevent.HandlerContext, msg hevent.Message, err error) error {
	gutil.PanicErr(err)

	var p HelloPayload
	gutil.PanicErr(msg.Payload.Decode(&p))

	hlog.CtxLogger(c).Info("ctx correlation_id", hlog.String("cid", hexa.CtxCorrelationId(c)))
	hlog.CtxLogger(c).Info(fmt.Sprintf("hi %s", p.Name))

	retryCount++
	if retryCount <= 3 {
		return errors.New("fake error just to retry the event")
	}
	hlog.CtxLogger(c).Info("ok, I'm processed this message, by :)")
	return nil
}
