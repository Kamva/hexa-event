package main

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	"github.com/kamva/hexa-event"
	"github.com/kamva/hexa-event/pulsar"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
)

type HelloPayload struct {
	Hello string `json:"hello"`
}

const clientURL = "pulsar://localhost:6650"
const format = "%s"
const channelName = "hexa-example"

var t = hexatranslator.NewEmptyDriver()
var l = hlog.NewPrinterDriver(hlog.DebugLevel)
var p = hexa.NewContextPropagator(l, t)

func main() {
	send()
	time.Sleep(2 * time.Second)
	receive()
}

func send() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: clientURL,
	})
	gutil.PanicErr(err)
	defer client.Close()

	var t = hexatranslator.NewEmptyDriver()
	var l = hlog.NewPrinterDriver(hlog.DebugLevel)

	emitter, err := hexapulsar.NewEmitter(client, hexapulsar.EmitterOptions{
		ProducerGenerator: hexapulsar.DefaultProducerGenerator(format),
		ContextPropagator: p,
		Encoder:           hevent.NewJsonEncoder(),
	})
	gutil.PanicErr(err)

	defer func() {
		gutil.PanicErr(emitter.Shutdown(context.Background()))
	}()

	event := &hevent.Event{
		Payload: HelloPayload{Hello: "from Hexa2:)"},
		Channel: channelName,
		Key:     "test-key",
	}

	ctx := hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId:  "test-correlation-id",
		Locale:         "en",
		User:           hexa.NewGuest(),
		BaseLogger:     l,
		BaseTranslator: t,
	})
	res, err := emitter.Emit(ctx, event)
	gutil.PanicErr(err)
	fmt.Println(res)
	fmt.Println("message sent :)")
}

func receive() {
	// From here for all receivers is same.
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: clientURL,
	})
	gutil.PanicErr(err)

	receiver, err := hexapulsar.NewReceiver(hexapulsar.ReceiverOptions{
		Client:            client,
		ContextPropagator: p,
	})
	gutil.PanicErr(err)

	defer func() {
		err := receiver.Shutdown(context.Background())
		gutil.PanicErr(err)
	}()

	err = receiver.Subscribe(channelName, sayHello)
	gutil.PanicErr(err)
	err = receiver.Run()
	gutil.PanicErr(err)
}

func sayHello(c hevent.HandlerContext, m hevent.Message, err error) error {
	gutil.PanicErr(err)
	fmt.Println("running hello handler.")
	fmt.Println(m.Headers)

	var p HelloPayload
	gutil.PanicErr(m.Payload.Decode(&p))

	fmt.Println(p.Hello)
	fmt.Println(hexa.CtxUser(c).Type())
	c.Ack()
	return nil
}

var _ hevent.EventHandler = sayHello
