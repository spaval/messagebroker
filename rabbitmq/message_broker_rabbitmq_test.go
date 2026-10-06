package rabbitmq

import (
	"errors"
	"testing"

	"github.com/streadway/amqp"
)

// recordingAcknowledger stands in for the channel a delivery came from, so the
// payload's functions can be exercised without a broker.
type recordingAcknowledger struct {
	calls []call
	err   error
}

type call struct {
	name     string
	tag      uint64
	multiple bool
	requeue  bool
}

func (a *recordingAcknowledger) Ack(tag uint64, multiple bool) error {
	a.calls = append(a.calls, call{name: "ack", tag: tag, multiple: multiple})
	return a.err
}

func (a *recordingAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	a.calls = append(a.calls, call{name: "nack", tag: tag, multiple: multiple, requeue: requeue})
	return a.err
}

func (a *recordingAcknowledger) Reject(tag uint64, requeue bool) error {
	a.calls = append(a.calls, call{name: "reject", tag: tag, requeue: requeue})
	return a.err
}

func delivery(acknowledger amqp.Acknowledger) amqp.Delivery {
	return amqp.Delivery{
		Acknowledger:  acknowledger,
		DeliveryTag:   42,
		Body:          []byte(`{"account":"16507"}`),
		RoutingKey:    "accounting.entries.info.dev",
		CorrelationId: "16507",
		ContentType:   "application/json",
		Exchange:      "avista_accounting_entries",
		DeliveryMode:  amqp.Persistent,
		Redelivered:   true,
	}
}

func TestNewPayloadCopiesTheMessage(t *testing.T) {
	msg := delivery(&recordingAcknowledger{})
	payload := newPayload(msg)

	if string(payload.Body) != string(msg.Body) {
		t.Errorf("Body: got %q, expected %q", payload.Body, msg.Body)
	}
	if payload.RoutingKey != msg.RoutingKey {
		t.Errorf("RoutingKey: got %q, expected %q", payload.RoutingKey, msg.RoutingKey)
	}
	if payload.CorrelationID != msg.CorrelationId {
		t.Errorf("CorrelationID: got %q, expected %q", payload.CorrelationID, msg.CorrelationId)
	}
	if payload.ContentType != msg.ContentType {
		t.Errorf("ContentType: got %q, expected %q", payload.ContentType, msg.ContentType)
	}
	if payload.Exchange != msg.Exchange {
		t.Errorf("Exchange: got %q, expected %q", payload.Exchange, msg.Exchange)
	}

	// DeliveryMode was declared on the payload but never filled in, so every
	// consumer saw a zero where the message said persistent.
	if payload.DeliveryMode != msg.DeliveryMode {
		t.Errorf("DeliveryMode: got %d, expected %d", payload.DeliveryMode, msg.DeliveryMode)
	}

	// Redelivered is what lets a consumer bound its retries.
	if payload.Redelivered != msg.Redelivered {
		t.Errorf("Redelivered: got %v, expected %v", payload.Redelivered, msg.Redelivered)
	}
}

func TestPayloadCarriesTheThreeAcknowledgements(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	payload := newPayload(delivery(acknowledger))

	if payload.Ack == nil || payload.Nack == nil || payload.Reject == nil {
		t.Fatalf("the payload must carry the three: ack=%v nack=%v reject=%v",
			payload.Ack != nil, payload.Nack != nil, payload.Reject != nil)
	}

	if err := payload.Ack(false); err != nil {
		t.Errorf("Ack returned %s", err)
	}

	// requeue true is the case that matters: after a transient failure the
	// broker has to redeliver the message instead of holding it.
	if err := payload.Nack(false, true); err != nil {
		t.Errorf("Nack returned %s", err)
	}

	if err := payload.Reject(false); err != nil {
		t.Errorf("Reject returned %s", err)
	}

	expected := []call{
		{name: "ack", tag: 42, multiple: false},
		{name: "nack", tag: 42, multiple: false, requeue: true},
		{name: "reject", tag: 42, requeue: false},
	}

	if len(acknowledger.calls) != len(expected) {
		t.Fatalf("got %d calls, expected %d: %+v", len(acknowledger.calls), len(expected), acknowledger.calls)
	}

	for i, want := range expected {
		if acknowledger.calls[i] != want {
			t.Errorf("call %d: got %+v, expected %+v", i, acknowledger.calls[i], want)
		}
	}
}

// The three return the acknowledger's error instead of swallowing it, so a
// consumer can tell that its confirmation did not reach the broker.
func TestTheAcknowledgementsReturnTheError(t *testing.T) {
	failing := errors.New("the channel is closed")
	payload := newPayload(delivery(&recordingAcknowledger{err: failing}))

	if err := payload.Ack(false); !errors.Is(err, failing) {
		t.Errorf("Ack: got %v, expected %v", err, failing)
	}
	if err := payload.Nack(false, true); !errors.Is(err, failing) {
		t.Errorf("Nack: got %v, expected %v", err, failing)
	}
	if err := payload.Reject(false); !errors.Is(err, failing) {
		t.Errorf("Reject: got %v, expected %v", err, failing)
	}
}

// A queue consumed with NoAck has no acknowledger. Calling any of the three
// must report that, not panic, because the consumer cannot know how the queue
// was set up.
func TestTheAcknowledgementsDoNotPanicWithoutAnAcknowledger(t *testing.T) {
	payload := newPayload(delivery(nil))

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("it panicked: %v", recovered)
		}
	}()

	if err := payload.Ack(false); err == nil {
		t.Error("Ack returned no error on a delivery with no acknowledger")
	}
	if err := payload.Nack(false, true); err == nil {
		t.Error("Nack returned no error on a delivery with no acknowledger")
	}
	if err := payload.Reject(false); err == nil {
		t.Error("Reject returned no error on a delivery with no acknowledger")
	}
}
