package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa-event/hestan"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

const (
	clusterID  = "test-cluster"
	clientID   = "stan-pub"
	clientName = "NATS Streaming Example Publisher"
	URL        = stan.DefaultNatsURL
)

var t = hexatranslator.NewEmptyDriver()
var l = hlog.NewPrinterDriver(hlog.DebugLevel)
var p = hexa.NewContextPropagator(l, t)

// Some other data
var (
	correlationID  = "my-correlation-id"
	channel        = "simple-channel"
	anotherChannel = "simple-channel-2"
	replyChannel   = "simple-channel-reply"
	msg            = "Hello from nats streaming broker :)"
)

type HelloPayload struct {
	Hello string `json:"hello"`
}

func main() {
	emitter, receiver := bootstrap()
	ctx := hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId: correlationID,
		Locale:        "en",
		User:          hexa.NewGuest(),
		Logger:        l,
		Translator:    t,
	})
	hlog.Info("connected...")

	receive(receiver)
	hlog.Info("listening to receive messages...")

	emit(ctx, emitter)
	hlog.Info("emitted...")

	waitToClose(emitter, receiver)
}

func bootstrap() (hevent.Emitter, hevent.Receiver) {
	nc, sc := connect()

	emitter, err := hestan.NewEmitter(hestan.EmitterOptions{
		NatsCon:           nc,
		StreamingCon:      sc,
		ContextPropagator: p,
		Encoder:           hevent.NewJsonEncoder(),
	})
	gutil.PanicErr(err)

	receiver, err := hestan.NewReceiver(hestan.ReceiverOptions{
		NatsCon:           nc,
		StreamingCon:      sc,
		ContextPropagator: p,
	})
	gutil.PanicErr(err)

	return emitter, receiver
}

func connect() (*nats.Conn, stan.Conn) {
	// Connect Options.
	opts := []nats.Option{nats.Name(clientName)}

	// Connect to NATS
	nc, err := nats.Connect(URL, opts...)
	if err != nil {
		hlog.Error("error for connection to NATS", hlog.Err(err))
	}

	sc, err := stan.Connect(clusterID, clientID, stan.NatsConn(nc))
	if err != nil {
		hlog.Error("Can't connect to NATS Streaming", hlog.Err(err), hlog.String("url", URL))
	}

	return nc, sc
}

func emit(ctx hexa.Context, emitter hevent.Emitter) {
	_, err := emitter.Emit(ctx, &hevent.Event{
		Key:          "my-key",
		Channel:      channel,
		ReplyChannel: replyChannel,
		Payload:      &HelloPayload{Hello: msg},
	})
	gutil.PanicErr(err)

	// Emit another message
	_, err = emitter.Emit(ctx, &hevent.Event{
		Key:          "my-key",
		Channel:      anotherChannel,
		ReplyChannel: replyChannel,
		Payload:      &HelloPayload{Hello: "another message :)"},
	})
	gutil.PanicErr(err)
}

func receive(receiver hevent.Receiver) {
	gutil.PanicErr(receiver.Subscribe(channel, handler))
	o := hestan.SubscriptionOptions{
		Subject:  anotherChannel,
		Group:    "group-1",
		Durable:  "my-durable",
		Position: stan.DeliverAllAvailable(),
		Handler:  handler,
	}
	gutil.PanicErr(receiver.SubscribeWithOptions(hestan.NewSubscriptionOptions(o)))
}

func handler(ctx hevent.HandlerContext, msg hevent.Message, err error) error {
	gutil.PanicErr(err)

	var p HelloPayload
	gutil.PanicErr(msg.Payload.Decode(&p))

	hlog.Info("correlation_id", hlog.String("correlation_id", ctx.CorrelationID()))
	hlog.Info("reply_channel", hlog.String("reply_channel", msg.ReplyChannel))
	hlog.Info("payload", hlog.String("payload", p.Hello))
	return err
}

func waitToClose(emitter hevent.Emitter, receiver hevent.Receiver) () {
	// Wait for a SIGINT (perhaps triggered by user with CTRL-C)
	// Run cleanup when signal is received
	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			hlog.Info("\nReceived an interrupt, unsubscribing and closing connection...\n\n")
			emitter.Shutdown(context.Background())
			receiver.Shutdown(context.Background())
			cleanupDone <- true
		}
	}()
	<-cleanupDone
}

var _ hevent.EventHandler = handler
