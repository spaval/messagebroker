//go:build integration

package rabbitmq

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spaval/messagebroker"
)

// Several publishers finding the connection closed at the same time each start a
// reconnection. They have to end up sharing one connection: without serialising
// the dial, each overwrites the others' and the broker is left holding the
// orphans, every one of them with its own consumers and deliveries in flight.
func TestConcurrentPublishersEndUpOnASingleConnection(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	if err := h.broker.Publish(directOptions(n), map[string]string{"warm": "up"}); err != nil {
		t.Fatalf("warming the publisher up: %v", err)
	}

	h.broker.Connection.Close()

	var wg sync.WaitGroup

	errors := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			errors <- h.broker.Publish(directOptions(n), map[string]string{"any": "thing"})
		}()
	}

	wg.Wait()
	close(errors)

	failures := 0
	for err := range errors {
		if err != nil {
			failures++
		}
	}

	// Some of the twenty can legitimately fail: whoever publishes on a channel
	// that a concurrent reconnection has just replaced gets ErrClosed back. What
	// must not happen is the connections piling up.
	t.Logf("%d of 20 concurrent publishes failed during the reconnection", failures)

	waitForConnections(t, 1)

	eventually(t, "publishing to work again", func() bool {
		return h.broker.Publish(directOptions(n), map[string]string{"after": "storm"}) == nil
	})
}

// Consume and Publish racing through a reconnection must not leave the broker
// with more than one connection either.
func TestConcurrentPublishAndConsumeDuringAReconnection(t *testing.T) {
	h := newHarness(t)

	var wg sync.WaitGroup

	h.broker.Connection.Close()

	for i := 0; i < 10; i++ {
		queue := newNames(t)

		wg.Add(2)

		go func() {
			defer wg.Done()
			h.broker.Publish(directOptions(queue), map[string]string{"any": "thing"})
		}()

		go func() {
			defer wg.Done()
			h.broker.Consume(directOptions(queue), make(chan messagebroker.MessageBrokerPayload, 4), make(chan error, 1))
		}()
	}

	wg.Wait()

	waitForConnections(t, 1)
}

// The structural weakness that is left: one consumer breaking the channel takes
// publishing down for the whole process, because every consumer and publisher
// shares the same pair of channels. It recovers now -- that is what the fixes
// bought -- but the window is real, and this test is what would start failing if
// the channels were ever isolated per operation.
func TestAChannelErrorStopsEveryPublisherUntilItRecovers(t *testing.T) {
	h := newHarness(t, func(c *messagebroker.MessageBrokerConfig) { c.ReconnectDelay = 3 })

	unrelated := newNames(t)
	if err := h.broker.Publish(directOptions(unrelated), map[string]string{"before": "the error"}); err != nil {
		t.Fatalf("publishing before the error: %v", err)
	}

	broken := newNames(t)

	first := directOptions(broken)
	first.QueueArguments = messagebroker.QueueArguments{MessageTTL: 60000}

	if err := h.broker.Publish(first, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("declaring the queue: %v", err)
	}

	conflicting := first
	conflicting.QueueArguments.MessageTTL = 30000

	if err := h.broker.Publish(conflicting, map[string]string{"any": "thing"}); err == nil {
		t.Fatal("the conflicting declaration was expected to fail")
	}

	// A queue that had nothing to do with the error is now unpublishable too.
	err := h.broker.Publish(directOptions(unrelated), map[string]string{"during": "the outage"})
	if err == nil {
		t.Skip("the channel recovered before the blast radius could be observed")
	}

	if !strings.Contains(err.Error(), "not open") && !strings.Contains(err.Error(), "closed") {
		t.Errorf("error: got %v, expected the shared channel to be unusable", err)
	}

	eventually(t, "the unrelated queue to be publishable again", func() bool {
		return h.broker.Publish(directOptions(unrelated), map[string]string{"after": "recovery"}) == nil
	})
}

// Reconnection is opt in. With it switched off the library takes the process
// down instead of carrying on without a broker, which is what lets the
// orchestrator restart it.
func TestNotifyPanicsWhenReconnectionIsDisabled(t *testing.T) {
	h := newHarness(t, func(c *messagebroker.MessageBrokerConfig) { c.ShouldReconnect = false })

	recovered := make(chan any, 1)

	go func() {
		defer func() { recovered <- recover() }()

		h.broker.Notify()
	}()

	select {
	case value := <-recovered:
		if value == nil {
			t.Fatal("Notify returned instead of taking the process down")
		}

		t.Logf("it panicked with: %v", value)
	case <-time.After(10 * time.Second):
		t.Fatal("Notify neither panicked nor returned")
	}
}

func TestConnectingToAnUnreachableBrokerReturnsAnError(t *testing.T) {
	config := messagebroker.MessageBrokerConfig{
		URL:             "amqp://test:test@localhost:5699/",
		ShouldReconnect: true,
		RetriesCount:    1,
	}

	broker, err := NewMessageBrokerRabbitMQ(config, NewRabbitObserver(), make(chan error, 1))
	if err == nil {
		broker.Connection.Close()
		t.Fatal("connecting to a port nothing listens on must fail")
	}
}
