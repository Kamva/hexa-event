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

const (
	Version = "2.3.0"
)

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
	gutil.PanicErr(err)
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
	sendEvent(emitter, "war")
	sendEvent(emitter, "tech")
	sendEvent(emitter, "tech")
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
		Key:     fmt.Sprintf("%s_key", topic),
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

	err = receiver.SubscribeWithOptions(hafka.NewSubscriptionOptions(hafka.ConsumerOptions{
		BootstrapServers: BootstrapServers,
		Config:           cfg,
		Topic:            "tech",
		RetryTopic:       "check_tech_message",
		Group:            "check_tech_message",
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

	var p HelloPayload
	gutil.PanicErr(msg.Payload.Decode(&p))

	hexa.Logger(c).Info("ctx correlation_id", hlog.String("cid", hexa.CtxCorrelationId(c)))
	hexa.Logger(c).Info(fmt.Sprintf("hi %s", p.Name))

	return errors.New("fake error just to retry the event")
}
