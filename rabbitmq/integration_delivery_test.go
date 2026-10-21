//go:build integration

package rabbitmq

import (
	"testing"
	"time"

	"github.com/spaval/messagebroker"
	"github.com/streadway/amqp"
)

func directOptions(n names) messagebroker.MessageBrokerDeliveryOptions {
	return messagebroker.MessageBrokerDeliveryOptions{
		QueueName:      n.Queue,
		MessageDurable: true,
	}
}

func subscribe(t *testing.T, h *harness, options messagebroker.MessageBrokerDeliveryOptions) chan messagebroker.MessageBrokerPayload {
	t.Helper()

	success := make(chan messagebroker.MessageBrokerPayload, 64)

	if err := h.broker.Consume(options, success, make(chan error, 1)); err != nil {
		t.Fatalf("consuming %s: %v", options.QueueName, err)
	}

	return success
}

// ------------------------------------------------------- publish / consume

func TestPublishAndConsumeThroughAnExchange(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)
	options := consumeOptions(n)

	success := subscribe(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"account": "16507"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	payload := receive(t, success)

	if got := string(payload.Body); got != `{"account":"16507"}` {
		t.Errorf("body: got %q", got)
	}
	if payload.RoutingKey != n.Routing {
		t.Errorf("routing key: got %q, expected %q", payload.RoutingKey, n.Routing)
	}
	if payload.Exchange != n.Exchange {
		t.Errorf("exchange: got %q, expected %q", payload.Exchange, n.Exchange)
	}
	if payload.Redelivered {
		t.Error("a first delivery must not be flagged as redelivered")
	}
}

// Without an exchange the library declares the queue and publishes through the
// default exchange, which routes on the queue name. This is the shape every
// ".error" queue in the platform uses.
func TestPublishAndConsumeDirectlyOnTheQueue(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)
	options := directOptions(n)

	success := subscribe(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"state": "rejected"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	payload := receive(t, success)

	if got := string(payload.Body); got != `{"state":"rejected"}` {
		t.Errorf("body: got %q", got)
	}
	if payload.Exchange != "" {
		t.Errorf("exchange: got %q, expected the default one", payload.Exchange)
	}
	if payload.RoutingKey != n.Queue {
		t.Errorf("routing key: got %q, expected the queue name %q", payload.RoutingKey, n.Queue)
	}
}

func TestPayloadCarriesTheMessageMetadata(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	success := subscribe(t, h, options)

	options.CorrelationID = "correlation-16507"

	if err := h.broker.Publish(options, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	payload := receive(t, success)

	if payload.CorrelationID != "correlation-16507" {
		t.Errorf("correlation id: got %q", payload.CorrelationID)
	}
	if payload.ContentType != "application/json" {
		t.Errorf("content type: got %q", payload.ContentType)
	}
	if payload.DeliveryMode != amqp.Persistent {
		t.Errorf("delivery mode: got %d, expected persistent", payload.DeliveryMode)
	}
}

func TestPublishMarshalsStructsAndSendsBytesUntouched(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)
	options := consumeOptions(n)

	success := subscribe(t, h, options)

	raw := []byte("not json at all")
	if err := h.broker.Publish(options, raw); err != nil {
		t.Fatalf("publishing bytes: %v", err)
	}

	if got := string(receive(t, success).Body); got != string(raw) {
		t.Errorf("bytes went through changed: got %q", got)
	}

	type score struct {
		Product string `json:"product"`
		Value   int    `json:"value"`
	}

	if err := h.broker.Publish(options, score{Product: "16507", Value: 720}); err != nil {
		t.Fatalf("publishing a struct: %v", err)
	}

	if got := string(receive(t, success).Body); got != `{"product":"16507","value":720}` {
		t.Errorf("struct was not marshalled as expected: got %q", got)
	}
}

// ------------------------------------------------------- acknowledgements

func TestAckRemovesTheMessageFromTheQueue(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)
	options := consumeOptions(n)

	success := subscribe(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	payload := receive(t, success)

	eventually(t, "the message to be in flight", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Unacked == 1
	})

	if err := payload.Ack(false); err != nil {
		t.Fatalf("acknowledging: %v", err)
	}

	eventually(t, "the queue to be empty", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Messages == 0 && state.Unacked == 0
	})
}

func TestNackWithRequeueGetsTheMessageDeliveredAgain(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)
	options := consumeOptions(n)

	success := subscribe(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	first := receive(t, success)
	if first.Redelivered {
		t.Error("the first delivery must not be flagged as redelivered")
	}

	if err := first.Nack(false, true); err != nil {
		t.Fatalf("nacking: %v", err)
	}

	second := receive(t, success)
	if !second.Redelivered {
		t.Error("a requeued message must come back flagged as redelivered")
	}

	if err := second.Ack(false); err != nil {
		t.Fatalf("acknowledging: %v", err)
	}
}

func TestNackWithoutRequeueDropsTheMessage(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)
	options := consumeOptions(n)

	success := subscribe(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	if err := receive(t, success).Nack(false, false); err != nil {
		t.Fatalf("nacking: %v", err)
	}

	expectNoDelivery(t, success, 2*time.Second)

	eventually(t, "the queue to be empty", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Messages == 0 && state.Unacked == 0
	})
}

func TestRejectBehavesLikeNackForASingleMessage(t *testing.T) {
	t.Run("with requeue", func(t *testing.T) {
		h := newHarness(t)
		n := newNames(t)
		options := consumeOptions(n)

		success := subscribe(t, h, options)

		if err := h.broker.Publish(options, map[string]string{"any": "thing"}); err != nil {
			t.Fatalf("publishing: %v", err)
		}

		if err := receive(t, success).Reject(true); err != nil {
			t.Fatalf("rejecting: %v", err)
		}

		if second := receive(t, success); !second.Redelivered {
			t.Error("a rejected and requeued message must come back flagged as redelivered")
		} else if err := second.Ack(false); err != nil {
			t.Fatalf("acknowledging: %v", err)
		}
	})

	t.Run("without requeue", func(t *testing.T) {
		h := newHarness(t)
		n := newNames(t)
		options := consumeOptions(n)

		success := subscribe(t, h, options)

		if err := h.broker.Publish(options, map[string]string{"any": "thing"}); err != nil {
			t.Fatalf("publishing: %v", err)
		}

		if err := receive(t, success).Reject(false); err != nil {
			t.Fatalf("rejecting: %v", err)
		}

		expectNoDelivery(t, success, 2*time.Second)
	})
}

// With NoAck the broker considers the message settled the moment it hands it
// over, so nothing is ever in flight.
func TestNoAckSettlesOnDelivery(t *testing.T) {
	h := newHarness(t)
	n := newNames(t)

	options := consumeOptions(n)
	options.NoAck = true

	success := subscribe(t, h, options)

	if err := h.broker.Publish(options, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	receive(t, success)

	eventually(t, "the queue to be empty", func() bool {
		state, ok := inspectQueue(t, n.Queue)
		return ok && state.Messages == 0 && state.Unacked == 0
	})
}

// A trap worth pinning down: amqp fills the acknowledger in even when the queue
// was consumed with NoAck, so acknowledging returns no error at all. The
// acknowledgement carries a delivery tag the broker never handed out and the
// broker answers by closing the channel.
//
// It used to be the whole process's channel. Now it is the subscription's own,
// so the damage stops there.
func TestAcknowledgingANoAckDeliveryOnlyKillsItsOwnSubscription(t *testing.T) {
	h := newHarness(t)

	careless := newNames(t)
	bystander := newNames(t)

	carelessOptions := consumeOptions(careless)
	carelessOptions.NoAck = true

	carelessStream := subscribe(t, h, carelessOptions)
	watching := newSubscriber(t, h, consumeOptions(bystander))

	if err := h.broker.Publish(carelessOptions, map[string]string{"any": "thing"}); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	if err := receive(t, carelessStream).Ack(false); err != nil {
		t.Errorf("acknowledging reported an error locally, expected none: %v", err)
	}

	// The subscription next door keeps working, and so does publishing.
	bystanderOptions := consumeOptions(bystander)

	eventually(t, "the bystander to still be publishable", func() bool {
		return h.broker.Publish(bystanderOptions, map[string]string{"still": "here"}) == nil
	})

	payload := receive(t, watching.deliveries)
	if err := payload.Ack(false); err != nil {
		t.Fatalf("acknowledging on the bystander: %v", err)
	}
}
