package rabbitmq

import (
	"reflect"
	"testing"

	"github.com/spaval/messagebroker"
	"github.com/streadway/amqp"
)

// Apps that have not adopted the new options must keep declaring the queue
// exactly as before: no arguments and every flag off.
func TestToAMQPTableWithoutArgumentsIsNil(t *testing.T) {
	if table := toAMQPTable(messagebroker.QueueArguments{}); table != nil {
		t.Errorf("expected nil, got %v", table)
	}
}

func TestToAMQPTableOmitsUnsetArguments(t *testing.T) {
	table := toAMQPTable(messagebroker.QueueArguments{XQueueType: "quorum"})

	expected := amqp.Table{"x-queue-type": "quorum"}
	if !reflect.DeepEqual(table, expected) {
		t.Errorf("got %v, expected %v", table, expected)
	}
}

func TestLegacyOptionsKeepPreviousDeclaration(t *testing.T) {
	// Same literal an app written for v0.0.9 uses; it must still compile and
	// leave the new options at their zero value.
	options := messagebroker.MessageBrokerDeliveryOptions{
		QueueName:      "task_q",
		MessageDurable: true,
	}

	if options.DeleteWhenUnused || options.Exclusive || options.NoWait {
		t.Error("new declaration flags must default to false")
	}
	if toAMQPTable(options.QueueArguments) != nil {
		t.Error("queue arguments must default to nil")
	}
}

func TestToAMQPTableMapsEveryArgument(t *testing.T) {
	table := toAMQPTable(messagebroker.QueueArguments{
		MessageTTL:           60000,
		DeadLetterExchange:   "dlx",
		DeadLetterRoutingKey: "task_q.dead",
		XQueueType:           "quorum",
	})

	expected := amqp.Table{
		"x-message-ttl":             60000,
		"x-dead-letter-exchange":    "dlx",
		"x-dead-letter-routing-key": "task_q.dead",
		"x-queue-type":              "quorum",
	}

	if !reflect.DeepEqual(table, expected) {
		t.Errorf("got %v, expected %v", table, expected)
	}

	// The client validates the table before sending it; an unsupported value
	// type would only fail at declaration time against a real broker.
	if err := table.Validate(); err != nil {
		t.Errorf("table does not pass client validation: %v", err)
	}
}
