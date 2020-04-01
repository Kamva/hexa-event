package hexapulsar

import (
	hevent "github.com/Kamva/hexa-event"
)

type (
	// SubscribeItemPack is a helper struct to help you to have both consumer options and subscription Item in single struct.
	// See its example in the "examples" directory.
	SubscribeItemPack struct {
		ConsumerOptions  ConsumerOptionsItem
		SubscriptionItem hevent.SubscriptionItem
	}
)

// DefaultSubscriptionItemPack returns new instance of the SubscribeItemPack
func DefaultSubscriptionItemPack(channel string, payloadInstance interface{}, handler hevent.EventHandler) SubscribeItemPack {
	return SubscribeItemPack{
		ConsumerOptions:  DefaultChannelOptions(channel),
		SubscriptionItem: hevent.DefaultSubscriptionItem(channel, payloadInstance, handler),
	}
}
