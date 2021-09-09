package hexapulsar

import (
	"errors"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/tracer"
)

// SubscriptionNameError is the error to specify both subscription_name and the topic_name is empty.
var SubscriptionNameError = errors.New("both subscriptionName and topic name are empty, we can not setOptionValues ")

type (
	// TopicFormatter is the formatter which can use to format topic.
	// e.g "persistent://my_tenant/hello/%s" , so if we has a topic
	// named "john" => topicFormatter format it and finally we have
	// "persistent://my_tenant/hello/john" as our topic
	TopicFormatter string

	// consumerOptionsGenerator Implement the ConsumerOptionsGenerator
	consumerOptionsGenerator struct {
	}
)

// Format format the topic
func (f TopicFormatter) Format(topic string) string {
	if topic == "" {
		return ""
	}
	return fmt.Sprintf(string(f), topic)
}

// FormatList format list of topics
func (f TopicFormatter) FormatList(topics []string) []string {
	if topics == nil {
		return nil
	}

	formattedTopics := make([]string, len(topics))
	for i, v := range topics {
		formattedTopics[i] = f.Format(v)
	}
	return formattedTopics
}

// Generate is the ConsumerOptionsGenerator function
func (g consumerOptionsGenerator) Generate(so *hevent.SubscriptionOptions) (pulsar.ConsumerOptions, error) {
	// Check if has any topicFormatter => format all topics.
	// check if dont has any option => setOptionValues an empty, otherwise keep its options
	// set options and return.
	var consumerOptions pulsar.ConsumerOptions
	var formatter TopicFormatter = "%s"

	for _, v := range so.Extra() {
		switch val := v.(type) {
		case pulsar.ConsumerOptions:
			consumerOptions = val
		case TopicFormatter:
			formatter = val
		}
	}

	return g.setOptionValues(generateOptions{
		formatter:       formatter,
		consumerOptions: consumerOptions,
		so:              so,
	})
}

// generateOptions is the options which we use to setOptionValues a consumerOptions.
type generateOptions struct {
	formatter       TopicFormatter
	consumerOptions pulsar.ConsumerOptions
	so              *hevent.SubscriptionOptions
}

// setOptionValues gets available options and set values on pulsar options.
// If you do not set the subscription name, it will be same as the topic(channel) name.
// If you do not set the subscription type, it will be "Exclusive".
func (g consumerOptionsGenerator) setOptionValues(o generateOptions) (pulsar.ConsumerOptions, error) {
	var consumerOptions = o.consumerOptions

	if consumerOptions.SubscriptionName == "" {
		if o.so.Channel == "" {
			return consumerOptions, tracer.Trace(SubscriptionNameError)
		}

		consumerOptions.SubscriptionName = o.so.Channel
	}

	// Generate Topics
	if consumerOptions.Topic == "" {
		consumerOptions.Topic = o.formatter.Format(o.so.Channel)
	}
	if len(consumerOptions.Topics) == 0 {
		consumerOptions.Topics = o.formatter.FormatList(o.so.Channels)
	}
	if consumerOptions.TopicsPattern != "" {
		consumerOptions.TopicsPattern = o.formatter.Format(o.so.ChannelsPattern)
	}

	return consumerOptions, nil
}

// SubscribeOptionsBuilder is a builder to build subscription options
// according to the pulsar options.
type SubscribeOptionsBuilder struct {
	formatter TopicFormatter
	o         pulsar.ConsumerOptions
	so        *hevent.SubscriptionOptions
}

// NewSubscribeOptionsBuilder returns new instance of the SubscriptionOptionsBuilderW.
func NewSubscribeOptionsBuilder(ch string, h hevent.EventHandler) *SubscribeOptionsBuilder {
	return &SubscribeOptionsBuilder{so: hevent.NewSubscriptionOptions(ch, h)}
}

// WithFormatter sets the formatter.
func (b *SubscribeOptionsBuilder) WithFormatter(f string) *SubscribeOptionsBuilder {
	b.formatter = TopicFormatter(f)
	return b
}

// WithSubscriptionName sets the subscription name.
func (b *SubscribeOptionsBuilder) WithSubscriptionName(sub string) *SubscribeOptionsBuilder {
	b.o.SubscriptionName = sub
	return b
}

// WithType sets the subscription type.
func (b *SubscribeOptionsBuilder) WithType(t pulsar.SubscriptionType) *SubscribeOptionsBuilder {
	b.o.Type = t
	return b
}

// WithOptions sets the pulsar consumer options.
func (b *SubscribeOptionsBuilder) WithOptions(o pulsar.ConsumerOptions) *SubscribeOptionsBuilder {
	b.o = o
	return b
}

// Build builds the hexa-event subscriptionOptions.
func (b *SubscribeOptionsBuilder) Build() *hevent.SubscriptionOptions {
	b.so.WithExtra(b.formatter)
	b.so.WithExtra(b.o)
	return b.so
}

// PulsarSubscribeOptions contains props which we can use to generate new SubscriptionOptions instance..
type PulsarSubscribeOptions struct {
	SubscriptionName string
	Formatter        string
	Channel          string
	Handler          hevent.EventHandler
	Type             pulsar.SubscriptionType
}

// NewSubscriptionFromPulsarOptions returns new instance of the SubscriptionOptions from the
// pulsar options.
func NewSubscriptionFromPulsarOptions(o PulsarSubscribeOptions) *hevent.SubscriptionOptions {
	return NewSubscribeOptionsBuilder(o.Channel, o.Handler).
		WithFormatter(o.Formatter).
		WithSubscriptionName(o.SubscriptionName).
		WithType(o.Type).
		Build()
}
