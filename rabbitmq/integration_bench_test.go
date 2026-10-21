//go:build integration

package rabbitmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/spaval/messagebroker"
)

// BenchmarkPublish measures what a publish costs end to end. Declaring the
// destination again for every message turns each one into a synchronous round
// trip, so the difference this reports is the round trip.
func BenchmarkPublish(b *testing.B) {
	t := &testing.T{}

	config := messagebroker.MessageBrokerConfig{
		URL:             brokerURL,
		ShouldReconnect: true,
		ReconnectDelay:  0,
		RetriesCount:    5,
	}

	broker, err := NewMessageBrokerRabbitMQ(config, NewRabbitObserver(), make(chan error, 8))
	if err != nil {
		b.Fatalf("connecting: %v", err)
	}
	defer broker.Connection.Close()

	queue := fmt.Sprintf("q.bench.%d", time.Now().UnixNano())
	defer deleteQueue(t, queue)

	options := messagebroker.MessageBrokerDeliveryOptions{
		QueueName:      queue,
		MessageDurable: true,
	}

	payload := map[string]string{"account": "16507", "state": "approved"}

	if err := broker.Publish(options, payload); err != nil {
		b.Fatalf("warming up: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := broker.Publish(options, payload); err != nil {
			b.Fatalf("publishing: %v", err)
		}
	}
}
