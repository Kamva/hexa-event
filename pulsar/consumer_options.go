package hexapulsar

import (
	"errors"
	"fmt"
	hevent "github.com/Kamva/hexa-event"
	"github.com/Kamva/tracer"
	"github.com/apache/pulsar-client-go/pulsar"
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
func (g consumerOptionsGenerator) setOptionValues(o generateOptions) (pulsar.ConsumerOptions, error) {
	var consumerOptions = o.consumerOptions

	if consumerOptions.SubscriptionName == "" && o.so.Channel == "" {
		return consumerOptions, tracer.Trace(SubscriptionNameError)
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

// WithExtraOptions sets extra options on the subscription options.
func WithExtraOptions(so *hevent.SubscriptionOptions, formatter, subName string) {
	WithFormatterOption(so, formatter)
	WithConsumerOptions(so, subName, pulsar.Exclusive)
}

// WithFormatterOption sets formatter on the subscription options.
func WithFormatterOption(so *hevent.SubscriptionOptions, formatter string) {
	so.WithExtra(TopicFormatter(formatter))
}

// WithConsumerOptions sets pulsar consumer options on the subscription options.
func WithConsumerOptions(so *hevent.SubscriptionOptions, subName string, subType pulsar.SubscriptionType) {
	WithConsumerFullOptions(so, pulsar.ConsumerOptions{
		SubscriptionName: subName,
		Type:             subType,
	})
}

// WithConsumerFullOptions sets pulsar consumer options on the subscription options.
func WithConsumerFullOptions(so *hevent.SubscriptionOptions, co pulsar.ConsumerOptions) {
	so.WithExtra(co)
}
