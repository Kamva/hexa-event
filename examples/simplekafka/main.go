package main

import (
	"context"
	"fmt"
	"os"
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
	topic="hi_salam2"
)

var BootstrapServers = []string{"localhost:9092"}
var l = hlog.NewPrinterDriver(hlog.InfoLevel)
var t = hexatranslator.NewEmptyDriver()
var p = hexa.NewContextPropagator(l, t)
var version = gutil.Must(sarama.ParseKafkaVersion(Version)).(sarama.KafkaVersion)

type HelloPayload struct {
	Name string `json:"name"`
}

func main() {
	hlog.SetGlobalLogger(l)
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
	defer emitter.Close()

	receiver, err := hafka.NewReceiver(hafka.ReceiverOptions{
		ContextPropagator: p,
		Client:            client,
	})

	gutil.PanicErr(err)
	defer receiver.Close()

	gutil.PanicErr(err)
	c, cancel := context.WithCancel(context.Background())
	_ = c
	//sendEvents(c, emitter, time.Second)
	subscribeToEvents(receiver)

	gutil.PanicErr(receiver.Start()) // receiver start non-blocking

	err = gutil.Wait(func(s os.Signal) error {
		cancel()
		hlog.Info("bye :)")
		return nil
	}, syscall.SIGINFO, syscall.SIGTERM)

	gutil.PanicErr(err)

}

func sendEvents(c context.Context, e hevent.Emitter, interval time.Duration) {
	hctx := hexa.NewContext(nil,hexa.ContextParams{
		CorrelationId: gutil.UUID(),
		Locale:        "en-US",
		User:          hexa.NewGuest(),
		Logger:        l,
		Translator:    t,
	})
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				_, err := e.Emit(hctx, &hevent.Event{
					Key:    gutil.UUID(),
					Channel: topic,
					Payload: &HelloPayload{
						Name: "ali",
					},
				})
				gutil.PanicErr(err)
				return
			case <-c.Done():
				return
			}
		}
	}()
}

func subscribeToEvents(receiver hevent.Receiver) {
	cfg := hafka.NewConfigWithDefaults(version, sarama.OffsetOldest)

	err := receiver.SubscribeWithOptions(hafka.NewSubscriptionOptions(hafka.ConsumerOptions{
		BootstrapServers: BootstrapServers,
		Config:           cfg,
		Topic:            topic,
		Group:            "check_hi_message",
		RetryPolicy:      hafka.DefaultRetryPolicy(),
		Handler:          helloHandler,
		PayloadInstance:  &HelloPayload{},
	}))
	gutil.PanicErr(err)
}

func helloHandler(hc hevent.HandlerContext, c hexa.Context, msg hevent.Message, err error) error {
	gutil.PanicErr(err)

	c.Logger().Info("msg headers", hlog.Any("headers", mapBytesToMapString(msg.Headers)))
	c.Logger().Info("ctx correlation_id", hlog.String("cid", c.CorrelationID()))
	c.Logger().Info(fmt.Sprintf("hi %s", msg.Payload.(*HelloPayload).Name))
	c.Logger().Info("Done message handing -------------")

	return nil
}

func mapBytesToMapString(b map[string][]byte) map[string]string {
	m := make(map[string]string)
	for k, v := range b {
		m[k] = string(v)
	}
	return m
}
