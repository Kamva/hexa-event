package hafka

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/kamva/gutil"
	"github.com/kamva/tracer"
)

type QueueManager interface {
	// RetryTopics returns the retry topics list.
	// e.g., order.created.retry.1 order.created.retry.2 order.created.retry.3
	RetryTopics() []string

	// DeadLetterQueue returns the dead letter queue name for the topic
	// e.g., order.created.dlq
	DeadLetterQueue() string

	// NextTopic returns the next topic name for failed message.
	// e.g., retriedCount is 2 and your max attempt value is 3 or more,
	// so it will returns order.created.retry.3 otherwise
	// it should return "order.created.dlq".
	NextTopic(retriedCount int) string

	// If you provide a.retry.4 as topic name, it will returns 4.
	// for default topic name without number it will should returns 0.
	RetryNumberFromTopic(topic string) (int, error)

	// RetryAfter send duration which we need to wait for the next retry.
	// If it's not the first retry,it's algorithm can be
	// "diff LastRetry and Min(BackoffCoefficient^(retriedCount-1)*InitialInterval,MaximumInterval)"
	// but for the fist time should return the InitialInterval.
	RetryAfter(retriedCount int, lastRetry time.Time) time.Duration
}

type queueManager struct {
	topic       string
	retryTopics []string
	p           RetryPolicy
}

func newQueueManager(topic string, p RetryPolicy) QueueManager {
	return &queueManager{
		topic: topic,
		p:     p,
	}
}

func (qm *queueManager) RetryTopics() []string {
	if qm.retryTopics != nil {
		return qm.retryTopics
	}

	qm.retryTopics = make([]string, qm.p.RetryTopicsCount)
	for i := 1; i <= qm.p.RetryTopicsCount; i++ {
		qm.retryTopics[i-1] = fmt.Sprintf("%s.retry.%d", qm.topic, i)
	}

	return qm.retryTopics
}

func (qm *queueManager) DeadLetterQueue() string {
	return fmt.Sprintf("%s.dlq", qm.topic)
}

func (qm *queueManager) RetryAfter(retriedCount int, lastRetry time.Time) time.Duration {
	if retriedCount < 0 {
		panic("retried count can not be negative")
	}

	interval := qm.p.InitialInterval * time.Duration(math.Pow(qm.p.BackoffCoefficient, float64(retriedCount)))
	interval = lastRetry.Add(interval).Sub(time.Now())

	return gutil.MinDuration(gutil.MaxDuration(interval, 0), qm.p.MaximumInterval)
}

func (qm *queueManager) RetryNumberFromTopic(topic string) (int, error) {
	if topic == "" {
		return 0, tracer.Trace(errors.New("topic can not be empty"))
	}

	snippets := strings.Split(topic, ".")
	attempts, err := strconv.Atoi(snippets[len(snippets)-1])

	// If we can not get number from the topic, so its the first
	// time and don't have any number.
	if err != nil {
		return 0, nil
	}

	return attempts, nil
}

func (qm *queueManager) NextTopic(retryCount int) string {
	if retryCount < 0 {
		panic("retry count can not be negative")
	}

	if retryCount >= qm.p.MaximumAttempts {
		return qm.DeadLetterQueue()
	}

	return qm.RetryTopics()[retryCount]
}
