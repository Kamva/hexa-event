package hafka

import "github.com/Shopify/sarama"

type ConfigOption func(cfg *sarama.Config)

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


