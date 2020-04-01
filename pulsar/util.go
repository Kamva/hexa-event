package hexapulsar

import (
	hevent "github.com/Kamva/hexa-event"
)

type (
	SubscribeItemPackList []SubscribeItemPack

	// SubscribeItemPack is a helper struct to help you to have both consumer options and subscription Item in single struct.
	// See its example in the "examples" directory.
	SubscribeItemPack struct {
		ConsumerOptions  ConsumerOptionsItem
		SubscriptionItem hevent.SubscriptionItem
	}
)

// ConsumerOptions returns list of Consumer options.
func (l SubscribeItemPackList) ConsumerOptions() []ConsumerOptionsItem {
	list := make([]ConsumerOptionsItem, len(l))
	for i, v := range l {
		list[i] = v.ConsumerOptions
	}
	return list
}

// SubscriptionItems returns list of SubscriptionItems
func (l SubscribeItemPackList) SubscriptionItems() []hevent.SubscriptionItem {
	list := make([]hevent.SubscriptionItem, len(l))
	for i, v := range l {
		list[i] = v.SubscriptionItem
	}
	return list
}

// DefaultSubscriptionItemPack returns new instance of the SubscribeItemPack
func DefaultSubscriptionItemPack(channel string, payloadInstance interface{}, handler hevent.EventHandler) SubscribeItemPack {
	return SubscribeItemPack{
		ConsumerOptions:  DefaultChannelOptions(channel),
		SubscriptionItem: hevent.DefaultSubscriptionItem(channel, payloadInstance, handler),
	}
}
