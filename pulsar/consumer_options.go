package hexapulsar

import (
	"fmt"
	hevent "github.com/Kamva/hexa-event"
	"github.com/Kamva/tracer"
	"github.com/apache/pulsar-client-go/pulsar"
)

type (
	// ConsumerOptionsItem contains options to generate new consumerOptions for specified channel.
	ConsumerOptionsItem struct {
		TopicNamingFormat string // We use this format to generate topics name.
		Channel           hevent.ChannelNames
		ConsumerOptions   pulsar.ConsumerOptions
	}

	// ConsumerOptionsGenerator generate new consumers.
	ConsumerOptionsGenerator func(client pulsar.Client, topic hevent.ChannelNames) (pulsar.ConsumerOptions, error)

	// defaultConsumerOptionsGenerator implements ConsumerOptionsGenerator function as its method.
	defaultConsumerOptionsGenerator struct {
		items []ConsumerOptionsItem
	}
)

func (cg *defaultConsumerOptionsGenerator) Generator(client pulsar.Client, topics hevent.ChannelNames) (pulsar.ConsumerOptions, error) {
	item := cg.findSubscriptionItem(topics.SubscriptionName)
	if item == nil {
		err := tracer.Trace(fmt.Errorf("pulsar option for the topic %s not found", topics.SubscriptionName))
		return pulsar.ConsumerOptions{}, err
	}

	return cg.SetConsumerTopicNames(item.TopicNamingFormat, item.ConsumerOptions, topics), nil
}

// SetConsumerTopicNames set the topic name on the options.
func (cg *defaultConsumerOptionsGenerator) SetConsumerTopicNames(format string, options pulsar.ConsumerOptions, topics hevent.ChannelNames) pulsar.ConsumerOptions {
	options.Name = topics.SubscriptionName

	if len(topics.Names) == 1 {
		options.Topic = cg.formatTopicNames(format, topics.Names[0])[0]
		return options
	}

	if len(topics.Names) > 1 {
		options.Topics = cg.formatTopicNames(format, topics.Names...)
		return options
	}

	options.TopicsPattern = cg.formatTopicNames(format, topics.Pattern)[0]
	return options
}

// formatTopicNames format the topic name relative to provided format.
func (cg *defaultConsumerOptionsGenerator) formatTopicNames(format string, names ...string) []string {
	finalNames := make([]string, len(names))
	for i, n := range names {
		finalNames[i] = fmt.Sprintf(format, n)
	}

	return finalNames
}

// findSubscriptionItem find subscription item in provided list.
func (cg *defaultConsumerOptionsGenerator) findSubscriptionItem(subscriptionName string) *ConsumerOptionsItem {
	for _, item := range cg.items {
		if item.Channel.SubscriptionName == subscriptionName {
			return &item
		}
	}

	return nil
}

// DefaultChannelOptions returns new instance of the subscriptionItem with default values.
// sample format can be "%s" or "persistent://public/{microservice_name}/%s"
func DefaultChannelOptions(format, channel string) ConsumerOptionsItem {
	return ConsumerOptionsItem{
		TopicNamingFormat: format,
		Channel:           hevent.NewChannelNames(channel, channel),
		ConsumerOptions:   ConsumerOptions(fmt.Sprintf("%s-sub", channel), pulsar.Exclusive),
	}
}

// ConsumerOptionsGeneratorByList get list of channels with their
// consumer options and return a consumer generator.
func NewConsumerOptionsGenerator(items []ConsumerOptionsItem) ConsumerOptionsGenerator {
	g := &defaultConsumerOptionsGenerator{items: items}

	return g.Generator
}

// ConsumerOptions returns new instance of pulsar consumer options.
func ConsumerOptions(name string, subscriptionType pulsar.SubscriptionType) pulsar.ConsumerOptions {
	return pulsar.ConsumerOptions{
		SubscriptionName: name,
		Type:             subscriptionType,
	}
}
