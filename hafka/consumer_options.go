package hafka

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/kamva/gutil"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
)

type ConsumerOption func(o *ConsumerOptions)

type ConsumerOptions struct {
	BootstrapServers []string
	Config           *sarama.Config
	Topic            string
	RetryTopic       string // The topic name that we push the message to retry it.
	Group            string // consumer group name
	RetryPolicy      RetryPolicy

	Handler         hevent.EventHandler
	PayloadInstance interface{}
}

// RetryPolicy defines the retry policy.
type RetryPolicy struct {
	// Backoff interval for the first retry. If BackoffCoefficient is 1.0 then it is used for all retries.
	// Required.
	// Default is 1.
	InitialInterval time.Duration

	// Coefficient used to calculate the next retry backoff interval.
	// The next retry interval is previous interval multiplied by this coefficient.
	// Must be 1 or larger.
	// Default is 2.0.
	BackoffCoefficient float64

	// Maximum backoff interval between retries. Exponential backoff leads to interval increase.
	// This value is the cap of the interval. Default is 100x of initial interval.
	MaximumInterval time.Duration

	// Maximum number of attempts. When exceeded the retries stop even if not expired yet.
	// If not set or set to 0, it means without retry. default is 3.
	MaximumAttempts int

	// default is value of MaximumAttempts, when you decrease
	// MaximumAttempts value, so you have old topics which has
	// topics which do not get more failed events but
	// has old failed evnets, using this field you can
	// listen to old topics. e.g., let's say count of MaximumAttempts
	// is 4, you change to 3 , so you has three topics, but you
	// need to listen to Topic 4 also for old failed evnets in
	// that Topic, until be sure that Topic is empty, then you
	// can change this field's value to 3.
	// must be equal or more than MaximumAttempts.
	RetryTopicsCount int // default is value of MaxRetry
}

func (rp RetryPolicy) Validate() error {
	if rp.InitialInterval < 0 {
		return tracer.Trace(errors.New("minimum value for initial interval is zero"))
	}
	if rp.BackoffCoefficient < 1 {
		return tracer.Trace(errors.New("minimum value for backoffCoefficient is 1"))
	}
	if rp.RetryTopicsCount < rp.MaximumAttempts {
		return tracer.Trace(errors.New("retry topics count can not be less than max attempts"))
	}
	return nil
}

var defaultRetryPolicy = RetryPolicy{
	InitialInterval:    time.Minute,
	BackoffCoefficient: 2,
	MaximumAttempts:    4,
}

func DefaultRetryPolicy() RetryPolicy {
	return defaultRetryPolicy
}

func (o ConsumerOptions) Validate() error {
	return validation.ValidateStruct(&o,
		validation.Field(&o.Config, validation.Required),
		validation.Field(&o.Topic, validation.Required),
		validation.Field(&o.RetryTopic, validation.Required),
		validation.Field(&o.Group, validation.Required),
		validation.Field(&o.RetryPolicy, validation.Required),
		validation.Field(&o.Handler, validation.Required),
		validation.Field(&o.PayloadInstance, validation.Required),
	)
}

func mergeWithDefaultConsumerOptions(o ConsumerOptions) ConsumerOptions {
	if o.RetryPolicy.MaximumInterval == 0 {
		o.RetryPolicy.MaximumInterval = o.RetryPolicy.InitialInterval * 100
	}

	o.RetryPolicy.RetryTopicsCount = gutil.Max(o.RetryPolicy.RetryTopicsCount, o.RetryPolicy.MaximumAttempts)
	return o
}

func NewSubscriptionOptions(o ConsumerOptions) *hevent.SubscriptionOptions {
	so := &hevent.SubscriptionOptions{
		Channel:         o.Topic,
		PayloadInstance: o.PayloadInstance,
		Handler:         o.Handler,
	}

	return so.WithExtra(&o)
}

// extractConsumerOptions get the consumer options rom hevent subscription options.
func extractConsumerOptions(o *hevent.SubscriptionOptions) *ConsumerOptions {
	for _, extra := range o.Extra() {
		if consumerOptions, ok := extra.(*ConsumerOptions); ok {
			return consumerOptions
		}
	}
	return nil
}

func retryPolicyLogFields(r RetryPolicy) []hlog.Field {
	return []hlog.Field{
		hlog.Duration("retry.initial_interval", r.InitialInterval),
		hlog.Any("retry.backoff_coefficient", r.BackoffCoefficient),
		hlog.Duration("retry.maximum_interval", r.MaximumInterval),
		hlog.Int("retry.maximum_attempts", r.MaximumAttempts),
		hlog.Int("retry.retry_topics_count", r.RetryTopicsCount),
	}
}
