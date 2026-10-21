//go:build integration

package rabbitmq

import (
	"strings"
	"testing"
	"time"

	"github.com/spaval/messagebroker"
)

func publishN(t *testing.T, h *harness, options messagebroker.MessageBrokerDeliveryOptions, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		if err := h.broker.Publish(options, map[string]int{"n": i}); err != nil {
			t.Fatalf("publishing message %d: %v", i, err)
		}
	}
}

// ---------------------------------------------------------------- prefetch

// With a prefetch the broker never hands over more than that many unacknowledged
// messages, so a consumer that stops acknowledging can only ever strand that
// many.
func TestPrefetchBoundsTheUnacknowledgedWindow(t *testing.T) {
	h := newHarness(t, func(c *messagebroker.MessageBrokerConfig) { c.PrefetchCount = 3 })
	n := newNames(t)
	options := consumeOptions(n)

	subscribe(t, h, options)
	publishN(t, h, options, 10)

	eventually(t, "the window to fill up to the prefetch", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Unacked == 3 && state.Ready == 7
	})

	// And it stays there: nothing is acknowledged, so nothing else is delivered.
	time.Sleep(2 * time.Second)

	state, _ := inspectQueue(t, n.Queue)
	if state.Unacked != 3 {
		t.Errorf("unacknowledged: got %d, expected it to stay at the prefetch of 3", state.Unacked)
	}
}

// This is the shape of the incident. PrefetchCount is left at zero -- which is
// what a commented out configuration line produces -- so Qos is never applied
// and the broker pushes the entire queue to the one consumer. Every message is
// in flight at once, and a consumer that does not acknowledge strands all of
// them instead of a bounded window.
func TestWithoutAPrefetchTheWholeQueueIsHandedOver(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)
	options := consumeOptions(n)

	subscribe(t, h, options)
	publishN(t, h, options, 25)

	eventually(t, "every message to be in flight at once", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Unacked == 25 && state.Ready == 0
	})
}

// ------------------------------------------------------- queue arguments

func TestQuorumQueueIsDeclaredAsQuorum(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	options.QueueArguments = messagebroker.QueueArguments{XQueueType: "quorum"}

	subscribe(t, h, options)

	eventually(t, "the queue to exist", func() bool {
		_, ok := inspectQueue(t, n.Queue)
		return ok
	})

	state, _ := inspectQueue(t, n.Queue)
	if state.Type != "quorum" {
		t.Errorf("type: got %q, expected quorum", state.Type)
	}
	if !state.Durable {
		t.Error("a quorum queue must be durable")
	}
}

// The defect yesterday's commit introduced across the platform: the error
// queues asked for a quorum type while leaving MessageDurable at its zero
// value. Quorum queues only exist durable, so the declaration is refused and
// the channel goes down with it.
func TestQuorumQueueWithoutDurableIsRefused(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := messagebroker.MessageBrokerDeliveryOptions{
		QueueName:      n.Queue,
		MessageDurable: false,
		QueueArguments: messagebroker.QueueArguments{XQueueType: "quorum"},
	}

	err := h.broker.Publish(options, map[string]string{"any": "thing"})
	if err == nil {
		t.Fatal("declaring a non durable quorum queue must fail")
	}

	t.Logf("the broker refused it with: %v", err)
}

// What every ".error" queue of the platform runs into on a current broker. They
// are declared with MessageDurable left at false, and from RabbitMQ 4.4 a non
// durable, non exclusive queue is not merely deprecated -- the warning a 4.3
// dashboard shows -- it is refused outright. Durability is not optional any
// more.
func TestTransientQueuesAreRefusedOutright(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := messagebroker.MessageBrokerDeliveryOptions{QueueName: n.Queue}

	err := h.broker.Publish(options, map[string]string{"any": "thing"})
	if err == nil {
		t.Skip("this broker still permits transient non exclusive queues")
	}

	if !strings.Contains(err.Error(), "transient_nonexcl_queues") {
		t.Fatalf("error: got %v, expected the transient_nonexcl_queues refusal", err)
	}
}

// Why the error queues cannot simply be switched over in code: the type is part
// of what the broker compares when a queue is declared again, and the queues
// already exist as classic.
func TestRedeclaringAQueueWithADifferentTypeIsRefused(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	classic := directOptions(n)
	if err := h.broker.Publish(classic, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("declaring the queue as classic: %v", err)
	}

	quorum := classic
	quorum.QueueArguments = messagebroker.QueueArguments{XQueueType: "quorum"}

	err := h.broker.Publish(quorum, map[string]string{"any": "thing"})
	if err == nil {
		t.Fatal("redeclaring a classic queue as quorum must fail")
	}

	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Errorf("error: got %v, expected PRECONDITION_FAILED", err)
	}
}

// Every argument is part of what the broker compares, not just the type. This is
// what rules out changing an existing queue's declaration from code at all: it
// has to be recreated.
func TestRedeclaringAQueueWithDifferentArgumentsIsRefused(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	first := directOptions(n)
	first.QueueArguments = messagebroker.QueueArguments{MessageTTL: 60000}

	if err := h.broker.Publish(first, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("declaring the queue: %v", err)
	}

	second := first
	second.QueueArguments.MessageTTL = 30000

	err := h.broker.Publish(second, map[string]string{"any": "thing"})
	if err == nil {
		t.Fatal("redeclaring a queue with a different ttl must fail")
	}

	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Errorf("error: got %v, expected PRECONDITION_FAILED", err)
	}
}

// The compatibility guarantee of the optional arguments: an empty
// QueueArguments must declare exactly the queue the library declared before they
// existed, or every queue already in production stops being declarable.
func TestEmptyQueueArgumentsDeclareTheQueueAsBefore(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	subscribe(t, h, options)

	eventually(t, "the queue to exist", func() bool {
		_, ok := inspectQueue(t, n.Queue)
		return ok
	})

	state, _ := inspectQueue(t, n.Queue)
	if state.Type != "classic" {
		t.Errorf("type: got %q, expected classic", state.Type)
	}

	// The broker echoes the effective x-queue-type back whatever was sent, so
	// what matters is that none of the optional arguments reached it.
	for _, argument := range []string{"x-message-ttl", "x-dead-letter-exchange", "x-dead-letter-routing-key", "x-delivery-limit"} {
		if _, sent := state.Arguments[argument]; sent {
			t.Errorf("%s was sent, expected an empty QueueArguments to send nothing", argument)
		}
	}
}

func TestMessageTTLExpiresMessages(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := directOptions(n)
	options.QueueArguments = messagebroker.QueueArguments{MessageTTL: 500}

	if err := h.broker.Publish(options, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	eventually(t, "the message to expire", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Messages == 0
	})
}

// A real dead letter exchange, as opposed to the hand written ".error" queues:
// a message discarded with Nack lands in it without the consumer publishing
// anything.
func TestDeadLetterExchangeReceivesDiscardedMessages(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	deadLetter := names{
		Queue:    n.Queue + ".dead",
		Exchange: n.Exchange + ".dead",
		Routing:  n.Routing + ".dead",
	}

	t.Cleanup(func() {
		deleteQueue(t, deadLetter.Queue)
		deleteExchange(t, deadLetter.Exchange)
	})

	// The dead letter side has to exist and be bound before anything is routed to it.
	deadLetterStream := subscribe(t, h, consumeOptions(deadLetter))

	options := consumeOptions(n)
	options.QueueArguments = messagebroker.QueueArguments{
		DeadLetterExchange:   deadLetter.Exchange,
		DeadLetterRoutingKey: deadLetter.Routing,
	}

	success := subscribe(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"account": "16507"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	if err := receive(t, success).Nack(false, false); err != nil {
		t.Fatalf("discarding: %v", err)
	}

	dead := receive(t, deadLetterStream)
	if got := string(dead.Body); got != `{"account":"16507"}` {
		t.Errorf("the dead lettered body changed: got %q", got)
	}

	if err := dead.Ack(false); err != nil {
		t.Fatalf("acknowledging the dead lettered message: %v", err)
	}
}

// ------------------------------------------------------- declaration cache

// Publishing used to declare the exchange or the queue again for every single
// message, paying a synchronous round trip per publish. It is declared once per
// connection now, which is visible from the outside: a queue deleted from under
// the application is no longer brought back by the next publish.
func TestPublishDeclaresTheQueueOnlyOnce(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := directOptions(n)

	if err := h.broker.Publish(options, map[string]string{"first": "one"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	eventually(t, "the queue to be declared", func() bool {
		_, ok := inspectQueue(t, n.Queue)
		return ok
	})

	deleteQueue(t, n.Queue)

	eventually(t, "the queue to be gone", func() bool {
		_, ok := inspectQueue(t, n.Queue)
		return !ok
	})

	if err := h.broker.Publish(options, map[string]string{"second": "one"}); err != nil {
		t.Fatalf("publishing again: %v", err)
	}

	// Nothing is routed anywhere and nothing is declared again.
	time.Sleep(2 * time.Second)

	if _, ok := inspectQueue(t, n.Queue); ok {
		t.Error("the queue was declared again, expected the declaration to be remembered")
	}
}

// What is remembered belongs to the channels, so a reconnection has to forget
// it. This is what bounds the staleness above to a single reconnection.
func TestAReconnectionForgetsWhatWasDeclared(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := directOptions(n)

	if err := h.broker.Publish(options, map[string]string{"first": "one"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	eventually(t, "the queue to be declared", func() bool {
		_, ok := inspectQueue(t, n.Queue)
		return ok
	})

	deleteQueue(t, n.Queue)
	killEveryConnection(t)

	eventually(t, "the queue to be declared again after the reconnection", func() bool {
		if err := h.broker.Publish(options, map[string]string{"after": "reconnect"}); err != nil {
			return false
		}

		_, ok := inspectQueue(t, n.Queue)
		return ok
	})
}

// Remembering must not hide a declaration that differs from the one already
// made: the broker still has to see it, and still has to refuse it.
func TestADeclarationThatDiffersIsStillSent(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	first := directOptions(n)
	first.QueueArguments = messagebroker.QueueArguments{MessageTTL: 60000}

	if err := h.broker.Publish(first, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("declaring the queue: %v", err)
	}

	// The very same declaration is remembered.
	if err := h.broker.Publish(first, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("publishing again with the same declaration: %v", err)
	}

	conflicting := first
	conflicting.QueueArguments.MessageTTL = 30000

	if err := h.broker.Publish(conflicting, map[string]string{"any": "thing"}); err == nil {
		t.Fatal("a declaration with different arguments must still reach the broker and be refused")
	}
}
