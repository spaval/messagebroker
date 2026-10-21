//go:build integration

package rabbitmq

import (
	"strings"
	"testing"
	"time"

	"github.com/spaval/messagebroker"
)

// breakTheChannel forces a channel level error the way the platform does by
// accident: declaring a queue that already exists with different arguments. The
// broker answers with PRECONDITION_FAILED and closes the channel.
func breakTheChannel(t *testing.T, h *harness, n names) {
	t.Helper()

	first := directOptions(n)
	first.QueueArguments = messagebroker.QueueArguments{MessageTTL: 60000}

	if err := h.broker.Publish(first, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("declaring the queue: %v", err)
	}

	conflicting := first
	conflicting.QueueArguments.MessageTTL = 30000

	err := h.broker.Publish(conflicting, map[string]string{"any": "thing"})
	if err == nil {
		t.Fatal("the conflicting declaration was expected to fail")
	}

	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Fatalf("expected PRECONDITION_FAILED, got %v", err)
	}
}

// The core of the incident. Dropping the connection makes amqp broadcast the
// close to the connection and to both channels at once, and the library used to
// read only the first of the three. The other two blocked the connection's
// reader goroutine for good, which is why the dashboard showed thousands of
// messages in flight with every rate at zero.
func TestConsumersKeepConsumingAfterTheConnectionIsDropped(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	consumer := newSubscriber(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"before": "outage"}); err != nil {
		t.Fatalf("publishing before the outage: %v", err)
	}

	if err := receive(t, consumer.deliveries).Ack(false); err != nil {
		t.Fatalf("acknowledging before the outage: %v", err)
	}

	killEveryConnection(t)

	eventually(t, "the consumer to subscribe again", func() bool {
		return consumer.resubscribes.load() > 0
	})

	eventually(t, "the queue to have a consumer again", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Consumers >= 1
	})

	eventually(t, "publishing to work again", func() bool {
		return h.broker.Publish(options, map[string]string{"after": "outage"}) == nil
	})

	payload := receive(t, consumer.deliveries)
	if !strings.Contains(string(payload.Body), "after") {
		t.Errorf("body: got %q, expected the message published after the outage", payload.Body)
	}

	if err := payload.Ack(false); err != nil {
		t.Fatalf("acknowledging after the outage: %v", err)
	}
}

// A channel level error used to be terminal: the probe the library used to ask
// whether a channel was closed reported it as open, so it never reconnected and
// every later publish of the process failed.
func TestPublishingRecoversAfterAChannelError(t *testing.T) {
	h := newHarness(t)
	broken := newNames(t)
	healthy := newNames(t)

	breakTheChannel(t, h, broken)

	eventually(t, "publishing to work again", func() bool {
		return h.broker.Publish(directOptions(healthy), map[string]string{"any": "thing"}) == nil
	})
}

// Two channel errors in a row on the same process. This is where the unread
// close listeners used to freeze the connection.
func TestASecondChannelErrorStillRecovers(t *testing.T) {
	h := newHarness(t)

	for round := 1; round <= 3; round++ {
		broken := newNames(t)
		breakTheChannel(t, h, broken)

		healthy := newNames(t)
		eventually(t, "publishing to work again", func() bool {
			return h.broker.Publish(directOptions(healthy), map[string]string{"round": "x"}) == nil
		})
	}
}

// The connection a reconnection replaces has to be released. Left open, the
// broker keeps its consumers and holds their unacknowledged deliveries on a
// connection the application no longer reads.
func TestReconnectDoesNotLeaveTheOldConnectionBehind(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	newSubscriber(t, h, consumeOptions(n))

	var before []string

	eventually(t, "the broker to report the first connection", func() bool {
		before = connectionNames(t)
		return len(before) > 0
	})

	breakTheChannel(t, h, newNames(t))

	// The reconnection opens a new connection, so the one it replaced has to
	// disappear instead of lingering with its consumers and their deliveries.
	eventually(t, "the replaced connection to be released", func() bool {
		current := connectionNames(t)

		for _, name := range current {
			for _, old := range before {
				if name == old {
					return false
				}
			}
		}

		return len(current) > 0
	})
}

// Whatever was in flight when the connection died has to come back, which is the
// behaviour that turns a missing acknowledgement into a redelivery rather than a
// lost message.
func TestUnacknowledgedMessagesComeBackAfterAnOutage(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	consumer := newSubscriber(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"account": "16507"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	first := receive(t, consumer.deliveries)
	if first.Redelivered {
		t.Error("the first delivery must not be flagged as redelivered")
	}

	// Deliberately not acknowledged.
	killEveryConnection(t)

	second := receive(t, consumer.deliveries)
	if !second.Redelivered {
		t.Error("a message that was in flight during an outage must come back flagged as redelivered")
	}

	if err := second.Ack(false); err != nil {
		t.Fatalf("acknowledging the redelivery: %v", err)
	}
}

// Publish and Consume reconnect on their own when they find the connection
// closed. They used to do it without telling the consumers, so the process came
// back with a working publisher and no consumer registered at the broker, while
// its queues filled up.
func TestAReconnectStartedByPublishResubscribesTheConsumers(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	consumer := newSubscriber(t, h, options)

	eventually(t, "the consumer to be registered", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Consumers == 1
	})

	// The application is kept from reacting to the close, so the only thing that
	// can bring the connection back is Publish's own check. Without this the
	// test proves nothing: Notify would resubscribe the consumers by itself.
	h.suppressReconnect()
	h.broker.Connection.Close()

	eventually(t, "publishing to reconnect by itself", func() bool {
		return h.broker.Publish(options, map[string]string{"after": "lazy reconnect"}) == nil
	})

	if consumer.resubscribes.load() == 0 {
		t.Fatal("the reconnection did not make the consumer subscribe again")
	}

	eventually(t, "the queue to have a consumer again", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Consumers == 1
	})

	payload := receive(t, consumer.deliveries)
	if err := payload.Ack(false); err != nil {
		t.Fatalf("acknowledging: %v", err)
	}
}

// Resubscribing belongs to building a new connection, not to being notified.
// Notifying every consumer on each close -- even the ones where nothing had to
// be rebuilt -- made them subscribe again on a channel that was already theirs,
// leaving the queue with one more consumer on every spurious notification.
func TestANotificationThatNeedsNoReconnectDoesNotStackConsumers(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	newSubscriber(t, h, consumeOptions(n))

	eventually(t, "the consumer to be registered", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Consumers == 1
	})

	// Nothing is closed, so there is nothing to rebuild and nobody to tell.
	for i := 0; i < 3; i++ {
		h.broker.Notify()
	}

	time.Sleep(6 * time.Second)

	state, _ := inspectQueue(t, n.Queue)
	if state.Consumers != 1 {
		t.Fatalf("the queue has %d consumers, expected the single one to be left alone", state.Consumers)
	}
}

// Requeueing is never bounded, whichever way the limit is configured. Proven
// here for the queue argument; a broker policy behaves the same. A consumer that
// hands back what it cannot process loops on it for good, so "give it back" is
// not a safe answer to a message that is not ours.
func TestRequeueingIsNotBoundedByTheDeliveryLimit(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	options.QueueArguments = messagebroker.QueueArguments{XQueueType: "quorum", DeliveryLimit: 3}

	success := subscribe(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"poison": "yes"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	deliveries := 0
	deadline := time.After(5 * time.Second)

	for {
		select {
		case payload := <-success:
			deliveries++

			if err := payload.Nack(false, true); err != nil {
				t.Fatalf("requeueing delivery %d: %v", deliveries, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("the broker stopped after %d deliveries; the limit is honoured after all and this test is stale", deliveries)
		case <-deadline:
			if deliveries <= 3 {
				t.Fatalf("only %d deliveries, expected the requeue loop to run past the limit of 3", deliveries)
			}

			t.Logf("the same message was delivered %d times with a delivery limit of 3", deliveries)

			return
		}
	}
}

// Each subscription gets a channel of its own, so a queue whose declaration the
// broker refuses costs only that subscription. They used to share one channel,
// and since the platform's services set their subscriptions up one after
// another, the first refusal took down the ones already running and every one
// that had not started yet -- five consumers lost to one bad queue argument.
func TestABadDeclarationOnlyCostsItsOwnSubscription(t *testing.T) {
	h := newHarness(t)

	existing := newNames(t)
	before := newNames(t)
	after := newNames(t)

	// A subscription that is already running when the refusal happens.
	running := newSubscriber(t, h, consumeOptions(before))

	// A queue that exists with arguments the next subscription will not match.
	// Declared without an exchange, so the declaration is the queue's own.
	settled := directOptions(existing)
	settled.QueueArguments = messagebroker.QueueArguments{MessageTTL: 60000}

	if err := h.broker.Publish(settled, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("declaring the queue: %v", err)
	}

	conflicting := settled
	conflicting.QueueArguments.MessageTTL = 30000

	if err := h.broker.Consume(conflicting, make(chan messagebroker.MessageBrokerPayload, 1), make(chan error, 1)); err == nil {
		t.Fatal("the conflicting subscription was expected to be refused")
	}

	// A subscription set up after the refusal, the way the ones further down
	// RegisterEvents would be.
	later := newSubscriber(t, h, consumeOptions(after))

	for _, c := range []struct {
		name string
		n    names
		s    *subscriber
	}{
		{"the subscription that was already running", before, running},
		{"the subscription set up after the refusal", after, later},
	} {
		options := consumeOptions(c.n)

		if err := h.broker.Publish(options, map[string]string{"queue": c.n.Queue}); err != nil {
			t.Fatalf("publishing to %s: %v", c.name, err)
		}

		payload := receive(t, c.s.deliveries)
		if err := payload.Ack(false); err != nil {
			t.Fatalf("acknowledging on %s: %v", c.name, err)
		}
	}
}

// The fail channel every subscription hands to Consume used to be written to by
// nobody. A subscription whose channel the broker closes is told through it now,
// so the application can see a dead subscription instead of silently receiving
// nothing.
func TestASubscriptionIsToldWhenItsChannelDies(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	options.NoAck = true

	success := make(chan messagebroker.MessageBrokerPayload, 8)
	fail := make(chan error, 4)

	if err := h.broker.Consume(options, success, fail); err != nil {
		t.Fatalf("consuming: %v", err)
	}

	if err := h.broker.Publish(options, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	// Acknowledging a delivery consumed with NoAck makes the broker close the
	// channel, which is the cheapest way to kill one subscription on purpose.
	if err := receive(t, success).Ack(false); err != nil {
		t.Fatalf("acknowledging: %v", err)
	}

	select {
	case err := <-fail:
		if !strings.Contains(err.Error(), n.Queue) {
			t.Errorf("error: got %v, expected it to name the queue %s", err, n.Queue)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the subscription was never told its channel had died")
	}
}
