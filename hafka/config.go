package hafka

import "github.com/Shopify/sarama"

type ConfigOption func(cfg *sarama.Config)


func NewConfigWithDefaults(version sarama.KafkaVersion, initialOffset int64) *sarama.Config {
	return NewConfig(
		WithVersion(version),
		WithInitialOffset(sarama.OffsetOldest),
	)
}

func NewConfig(options ...ConfigOption) *sarama.Config {
	cfg := sarama.NewConfig()
	for _, o := range options {
		o(cfg)
	}
	return cfg
}

//--------------------------------
// Config functions
//--------------------------------

func WithVersion(v sarama.KafkaVersion) ConfigOption {
	return func(cfg *sarama.Config) {
		cfg.Version = v
	}
}

func WithInitialOffset(offset int64) ConfigOption {
	return func(cfg *sarama.Config) {
		cfg.Consumer.Offsets.Initial = offset
	}
}
