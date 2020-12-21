package hafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueueManager_RetryTopics(t *testing.T) {
	qm := newQueueManager("abc", RetryPolicy{
		InitialInterval:    1,
		BackoffCoefficient: 1,
		MaximumAttempts:    1,
		RetryTopicsCount:   3,
	})
	assert.Equal(t, qm.RetryTopics(), []string{
		"abc.retry.1",
		"abc.retry.2",
		"abc.retry.3",
	})
}

func TestQueueManager_RetryTopics2(t *testing.T) {
	qm := newQueueManager("abc", RetryPolicy{
		InitialInterval:    1,
		BackoffCoefficient: 1,
		MaximumAttempts:    10,
		RetryTopicsCount:   3,
	})
	assert.Equal(t, []string{
		"abc.retry.1",
		"abc.retry.2",
		"abc.retry.3",
	}, qm.RetryTopics())
}

func TestQueueManager_DeadLetterQueue(t *testing.T) {
	qm := newQueueManager("abc", RetryPolicy{
		InitialInterval:    1,
		BackoffCoefficient: 1,
		MaximumAttempts:    10,
		RetryTopicsCount:   3,
	})
	assert.Equal(t, "abc.dlq", qm.DeadLetterQueue())
}

func TestQueueManager_NextTopic(t *testing.T) {
	qm := newQueueManager("abc", RetryPolicy{
		InitialInterval:    1,
		BackoffCoefficient: 1,
		MaximumAttempts:    3,
		RetryTopicsCount:   3,
	})

	assert.Panics(t, func() {
		qm.NextTopic(-1)
	})

	assert.Equal(t, "abc.retry.1", qm.NextTopic(0))
	assert.Equal(t, "abc.retry.2", qm.NextTopic(1))
	assert.Equal(t, "abc.retry.3", qm.NextTopic(2))
	assert.Equal(t, "abc.dlq", qm.NextTopic(3))
	assert.Equal(t, "abc.dlq", qm.NextTopic(4))
}

func TestQueueManager_RetryNumberFromTopic(t *testing.T) {
	qm := newQueueManager("abc", RetryPolicy{
		InitialInterval:    1,
		BackoffCoefficient: 1,
		MaximumAttempts:    3,
		RetryTopicsCount:   3,
	})
	l := []struct {
		Tag   string
		Topic string
		Num   int
		Err   bool
	}{
		{Tag: "test.1", Topic: "", Err: true},
		{Tag: "test.2", Topic: "abc", Num: 0},
		{Tag: "test.3", Topic: "abc.retry.1", Num: 1},
		{Tag: "test.4", Topic: "abc.retry.2", Num: 2},
	}

	for _, in := range l {
		num, err := qm.RetryNumberFromTopic(in.Topic)
		if in.Err {
			assert.Error(t, err, in.Tag)
			continue
		}
		assert.Equal(t, in.Num, num, in.Tag)
	}
}

func TestQueueManager_RetryAfter(t *testing.T) {
	qm := newQueueManager("abc", RetryPolicy{
		InitialInterval:    time.Second * 2,
		MaximumInterval:    time.Second * 200,
		BackoffCoefficient: 2,
		MaximumAttempts:    3,
		RetryTopicsCount:   3,
	})
	now := time.Now()

	l := []struct {
		Tag        string
		RetryCount int
		LastRetry  time.Time
		Expected   time.Duration
		Panic      bool
	}{
		{Tag: "test.1", RetryCount: -1, Panic: true},
		{Tag: "test.2", RetryCount: 0, LastRetry: now, Expected: time.Second * 2},
		{Tag: "test.3", RetryCount: 1, LastRetry: now, Expected: time.Second * 4},
		{Tag: "test.4", RetryCount: 2, LastRetry: now, Expected: time.Second * 8},
		{Tag: "test.5", RetryCount: 3, LastRetry: now, Expected: time.Second * 16},
		{Tag: "test.6", RetryCount: 4, LastRetry: now, Expected: time.Second * 32},
		{Tag: "test.7", RetryCount: 10, LastRetry: now, Expected: time.Second * 200}, // should return maxInterval
	}

	for _, in := range l {
		if in.Panic {
			assert.Panics(t, func() {
				qm.RetryAfter(in.RetryCount, in.LastRetry)
			}, in.Tag)
			continue
		}

		after := qm.RetryAfter(in.RetryCount, in.LastRetry)

		// specified time should be equal to extended time or maximum one second less than it.
		assert.True(t, in.Expected >= after, "tag: %s, expected: %s, value: %s", in.Tag, in.Expected, after)
		assert.True(t, in.Expected-time.Second <= after, "tag: %s, expected: %s, value: %s", in.Tag, in.Expected, after)
	}
}