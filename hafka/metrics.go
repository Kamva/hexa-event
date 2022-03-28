package hafka

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/kamva/hexa-event"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

const instrumentationName = "github.com/hexa/hevent/hafka"

type MetricsConfig struct {
	MeterProvider metric.MeterProvider
	ServerName    string
}

func MetricsMiddleware(cfg MetricsConfig) hevent.Middleware {
	meter := cfg.MeterProvider.Meter(instrumentationName)
	meterMust := metric.Must(meter)
	eventCounter := meterMust.NewFloat64Counter("received_events_total")
	failedCounter := meterMust.NewFloat64Counter("failed_events_total")
	processDuration := meterMust.NewFloat64Histogram("event_consume_duration_seconds")
	payloadSizeBytes := meterMust.NewInt64Histogram("event_payload_size_bytes")

	return func(next hevent.EventHandler) hevent.EventHandler {
		return func(c hevent.HandlerContext, message hevent.Message, err error) error {
			startTime := time.Now()
			primaryMsg := message.Primary.(*sarama.ConsumerMessage)

			err = next(c, message, err)
			attrs := []attribute.KeyValue{
				semconv.MessagingSystemKey.String("kafka"),
				semconv.MessagingDestinationKey.String(primaryMsg.Topic),
				hevent.MessagingActionName.String(c.Value(hevent.HexaEventHandlerActionName).(string)),
				hevent.MessagingRootActionName.String(c.Value(hevent.HexaRootEventHandlerActionName).(string)),
				hevent.MessagingWithError.Bool(err != nil),
			}

			elapsed := float64(time.Since(startTime)) / float64(time.Second)
			measurements := []metric.Measurement{
				eventCounter.Measurement(1),
				processDuration.Measurement(elapsed),
				payloadSizeBytes.Measurement(int64(len(primaryMsg.Value))),
			}
			if err != nil {
				measurements = append(measurements, failedCounter.Measurement(1))
			}
			meter.RecordBatch(c, attrs, measurements...)
			return err
		}
	}
}
