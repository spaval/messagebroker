//go:build integration

package rabbitmq

import (
	"testing"
	"time"

	"github.com/spaval/messagebroker"
)

func subscribeWithHandle(t *testing.T, h *harness, options messagebroker.MessageBrokerDeliveryOptions, success chan messagebroker.MessageBrokerPayload) messagebroker.Subscription {
	t.Helper()

	handle, err := h.broker.Subscribe(options, success, make(chan error, 4))
	if err != nil {
		t.Fatalf("subscribing to %s: %v", options.QueueName, err)
	}

	return handle
}

// Stopping has to leave nothing behind at the broker. Without a handle a
// consumer that resubscribes -- which these do on every reconnection -- piles
// the previous subscription up instead of replacing it.
func TestStoppingASubscriptionEndsItAtTheBroker(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	success := make(chan messagebroker.MessageBrokerPayload, 8)

	handle := subscribeWithHandle(t, h, options, success)

	eventually(t, "the consumer to be registered", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Consumers == 1
	})

	if err := handle.Stop(); err != nil {
		t.Fatalf("stopping: %v", err)
	}

	eventually(t, "the consumer to be gone", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Consumers == 0
	})

	if err := h.broker.Publish(options, map[string]string{"after": "stop"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	expectNoDelivery(t, success, 3*time.Second)
}

// Resubscribing the way a consumer would across a reconnection: stop the old
// one, start a new one, and the queue is left with exactly one consumer.
func TestResubscribingWithAHandleDoesNotStackConsumers(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	success := make(chan messagebroker.MessageBrokerPayload, 8)

	var handle messagebroker.Subscription

	for round := 0; round < 4; round++ {
		if handle != nil {
			if err := handle.Stop(); err != nil {
				t.Fatalf("stopping round %d: %v", round, err)
			}
		}

		handle = subscribeWithHandle(t, h, options, success)
	}

	eventually(t, "a single consumer to be left", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Consumers == 1
	})

	time.Sleep(6 * time.Second)

	state, _ := inspectQueue(t, n.Queue)
	if state.Consumers != 1 {
		t.Fatalf("the queue has %d consumers after four rounds, expected one", state.Consumers)
	}

	if err := h.broker.Publish(options, map[string]string{"still": "working"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	if err := receive(t, success).Ack(false); err != nil {
		t.Fatalf("acknowledging: %v", err)
	}
}

func TestStoppingIsSafeToRepeat(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	handle := subscribeWithHandle(t, h, consumeOptions(n), make(chan messagebroker.MessageBrokerPayload, 4))

	for i := 0; i < 3; i++ {
		if err := handle.Stop(); err != nil {
			t.Fatalf("stop %d: %v", i+1, err)
		}
	}
}

// Stopping must not wait for whoever reads the payloads. A consumer that has
// stopped reading is exactly when stopping matters, and the goroutine feeding
// the channel is blocked on a send at that point.
func TestStoppingDoesNotWaitForAReaderThatIsGone(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)

	// One slot, and nobody reading it: the feeding goroutine fills it and blocks.
	success := make(chan messagebroker.MessageBrokerPayload, 1)
	handle := subscribeWithHandle(t, h, options, success)

	for i := 0; i < 5; i++ {
		if err := h.broker.Publish(options, map[string]int{"n": i}); err != nil {
			t.Fatalf("publishing: %v", err)
		}
	}

	eventually(t, "the feeder to block on a full channel", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Unacked > 0
	})

	stopped := make(chan error, 1)

	go func() { stopped <- handle.Stop() }()

	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stopping: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stopping blocked on the payload channel")
	}
}

// Whatever the subscription held and had not acknowledged belongs back in the
// queue, so stopping mid flight loses nothing.
func TestStoppingReturnsWhatWasNotAcknowledged(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	success := make(chan messagebroker.MessageBrokerPayload, 8)

	handle := subscribeWithHandle(t, h, options, success)

	if err := h.broker.Publish(options, map[string]string{"account": "16507"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	receive(t, success)

	eventually(t, "the message to be in flight", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Unacked == 1
	})

	if err := handle.Stop(); err != nil {
		t.Fatalf("stopping: %v", err)
	}

	eventually(t, "the message to be back in the queue", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Ready == 1 && state.Unacked == 0
	})
}

// Stopping one subscription leaves the others alone, now that each has its own
// channel.
func TestStoppingOneSubscriptionLeavesTheOthersAlone(t *testing.T) {
	h := newHarness(t)

	stopped := newNames(t)
	kept := newNames(t)

	stoppedStream := make(chan messagebroker.MessageBrokerPayload, 4)
	keptStream := make(chan messagebroker.MessageBrokerPayload, 4)

	handle := subscribeWithHandle(t, h, consumeOptions(stopped), stoppedStream)
	subscribeWithHandle(t, h, consumeOptions(kept), keptStream)

	if err := handle.Stop(); err != nil {
		t.Fatalf("stopping: %v", err)
	}

	keptOptions := consumeOptions(kept)
	if err := h.broker.Publish(keptOptions, map[string]string{"still": "here"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	if err := receive(t, keptStream).Ack(false); err != nil {
		t.Fatalf("acknowledging: %v", err)
	}
}
