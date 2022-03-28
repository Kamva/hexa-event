package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"syscall"
	"time"

	selector "go.opentelemetry.io/otel/sdk/metric/selector/simple"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"github.com/Shopify/sarama"
	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa-event/hafka"
	"github.com/kamva/hexa/hexatranslator"
	"github.com/kamva/hexa/hlog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/sdk/export/metric/aggregation"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/histogram"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	service     = "hexa-event-demo"
	environment = "dev"
	id          = 1

	Version = "2.3.0"
	topic   = "hi_salam2"
)

var BootstrapServers = []string{"localhost:9092"}
var l = hlog.NewPrinterDriver(hlog.InfoLevel)
var t = hexatranslator.NewEmptyDriver()
var p = hexa.NewContextPropagator(l, t)
var version = gutil.Must(sarama.ParseKafkaVersion(Version)).(sarama.KafkaVersion)

type HelloPayload struct {
	Name string `json:"name"`
}

func initMeter() {
	config := prometheus.Config{}
	c := controller.New(
		processor.NewFactory(
			selector.NewWithHistogramDistribution(
				histogram.WithExplicitBoundaries(config.DefaultHistogramBoundaries),
			),
			aggregation.CumulativeTemporalitySelector(),
			processor.WithMemory(true),
		),
		controller.WithResource(resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceNameKey.String(service),
			attribute.String("environment", environment),
			attribute.Int64("ID", id))),
	)

	exporter, err := prometheus.New(config, c)

	if err != nil {
		log.Panicf("failed to initialize prometheus exporter %v", err)
	}
	global.SetMeterProvider(exporter.MeterProvider())

	http.HandleFunc("/", exporter.ServeHTTP)
	go func() {
		_ = http.ListenAndServe(":2222", nil)
	}()

	fmt.Println("Prometheus server running on :2222")
}

func main() {
	hlog.SetGlobalLogger(l)
	initMeter()

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

	metrics := hafka.MetricsMiddleware(hafka.MetricsConfig{
		MeterProvider: global.GetMeterProvider(),
		ServerName:    "simple-server",
	})

	receiver, err := hafka.NewReceiver(hafka.ReceiverOptions{
		ContextPropagator: p,
		Client:            client,
		Middlewares:       []hevent.Middleware{metrics, hevent.RecoverMiddleware},
	})

	gutil.PanicErr(err)
	defer receiver.Shutdown(context.Background())

	gutil.PanicErr(err)
	c, cancel := context.WithCancel(context.Background())
	_ = c
	sendEvents(c, emitter, time.Second)
	subscribeToEvents(receiver)

	gutil.PanicErr(receiver.Run()) // receiver start non-blocking

	err = gutil.Wait(func(s os.Signal) error {
		cancel()
		hlog.Info("bye :)")
		return nil
	}, syscall.SIGINFO, syscall.SIGTERM)

	gutil.PanicErr(err)
}

func sendEvents(c context.Context, e hevent.Emitter, interval time.Duration) {
	hctx := hexa.NewContext(nil, hexa.ContextParams{
		CorrelationId:  gutil.UUID(),
		Locale:         "en-US",
		User:           hexa.NewGuest(),
		BaseLogger:     l,
		BaseTranslator: t,
	})
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				_, err := e.Emit(hctx, &hevent.Event{
					Key:     gutil.UUID(),
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
		RetryTopic:       "check_hi_message",
		Group:            "check_hi_message",
		RetryPolicy:      hafka.DefaultRetryPolicy(),
		Handler:          helloHandler,
	}))
	gutil.PanicErr(err)

	err = receiver.SubscribeWithOptions(hafka.NewSubscriptionOptions(hafka.ConsumerOptions{
		BootstrapServers: BootstrapServers,
		Config:           cfg,
		Topic:            topic,
		RetryTopic:       "say_hi_message",
		Group:            "say_hi_message",
		RetryPolicy:      hafka.DefaultRetryPolicy(),
		Handler:          sayHandler,
	}))
	gutil.PanicErr(err)
}

func helloHandler(c hevent.HandlerContext, msg hevent.Message, err error) error {
	var p HelloPayload
	gutil.PanicErr(msg.Payload.Decode(&p))

	l := hlog.CtxLogger(c)
	l.Info("msg headers", hlog.Any("headers", mapBytesToMapString(msg.Headers)))
	l.Info("ctx correlation_id", hlog.String("cid", hexa.CtxCorrelationId(c)))
	l.Info(fmt.Sprintf("hi %s", p.Name))
	logDeduplicatorMetaValues(c)
	l.Info("Done message handing \n -------------")

	return nil
}

func sayHandler(c hevent.HandlerContext, msg hevent.Message, err error) error {
	panic("paniccccccc")
}

func logDeduplicatorMetaValues(c context.Context) {
	hlog.Info("event id", hlog.String("event_id", c.Value(hevent.HexaEventID).(string)))
	hlog.Info("action name", hlog.String("action_name", c.Value(hevent.HexaEventHandlerActionName).(string)))
	hlog.Info("root event id", hlog.String("root_event_id", c.Value(hevent.HexaRootEventID).(string)))
	hlog.Info("root action name", hlog.String("root_action_name", c.Value(hevent.HexaRootEventHandlerActionName).(string)))
}

func mapBytesToMapString(b map[string][]byte) map[string]string {
	m := make(map[string]string)
	for k, v := range b {
		m[k] = string(v)
	}
	return m
}
