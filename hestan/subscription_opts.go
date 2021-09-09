package hestan

import (
	hevent "github.com/kamva/hexa-event"
	"github.com/nats-io/stan.go"
)

type SubscriptionOptions struct {
	Subject  string
	Group    string // provide it if you want to subscribe using queue group.
	Durable  string // provide it if you want durable subscription Position
	Position stan.SubscriptionOption
	Opts     []stan.SubscriptionOption

	Handler         hevent.EventHandler
}

func NewSubscriptionOptions(o SubscriptionOptions) *hevent.SubscriptionOptions {
	so := &hevent.SubscriptionOptions{
		Channel:         o.Subject,
		Handler:         o.Handler,
	}
	return so.WithExtra(o)
}
