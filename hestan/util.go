package hestan

import (
	"fmt"
	"github.com/kamva/gutil"
	"github.com/nats-io/stan.go"
	"time"
)

// UniqueClientID generates a new client id formatted by
// this pattern: {prefix}-{data}-{rand}  e.g., accounting-ms-2020101318281-hn1ig8
func UniqueClientID(prefix string) string {
	if len(prefix) != 0 {
		prefix = prefix + "-"
	}

	return fmt.Sprintf("%s%s", prefix, gutil.RandWithDate(time.Now(), 6))
}

func ManualAckOption(waitDuration time.Duration) []stan.SubscriptionOption {
	return []stan.SubscriptionOption{
		stan.SetManualAckMode(),
		stan.AckWait(time.Second * waitDuration),
	}
}
