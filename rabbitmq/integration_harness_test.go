//go:build integration

// Integration tests. They need a real broker:
//
//	podman run -d --name mb-rabbit -p 5672:5672 -p 15672:15672 \
//	  -e RABBITMQ_DEFAULT_USER=test -e RABBITMQ_DEFAULT_PASS=test rabbitmq:4-management
//	go test ./rabbitmq/ -tags integration -count=1
//
// Assertions about what the broker actually did -- how many messages are
// unacknowledged, how many consumers a queue has, how many connections the
// process holds -- go through the management API, because that is the only
// place where the library's effects are visible from outside.
package rabbitmq

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spaval/messagebroker"
)

// The broker to test against. Point them elsewhere to run the same suite on
// another version:
//
//	MB_AMQP_URL=amqp://test:test@localhost:5673/ \
//	MB_MANAGEMENT_URL=http://localhost:15673/api \
//	  go test ./rabbitmq/ -tags integration -count=1
var (
	brokerURL      = envOr("MB_AMQP_URL", "amqp://test:test@localhost:5672/")
	managementURL  = envOr("MB_MANAGEMENT_URL", "http://localhost:15672/api")
	managementUser = envOr("MB_MANAGEMENT_USER", "test")
	managementPass = envOr("MB_MANAGEMENT_PASS", "test")
)

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

// ---------------------------------------------------------------- harness

// harness wires a broker the way the applications do: a closing channel that a
// goroutine drains by calling Notify, which is what drives reconnection.
type harness struct {
	t        *testing.T
	broker   *MessageBrokerRabbitMQ
	observer *RabbitObserver
	closing  chan error

	mu       sync.Mutex
	notified []error
	stopped  bool
}

func newHarness(t *testing.T, mutate ...func(*messagebroker.MessageBrokerConfig)) *harness {
	t.Helper()

	config := messagebroker.MessageBrokerConfig{
		URL:             brokerURL,
		ShouldReconnect: true,
		ReconnectDelay:  0,
		RetriesCount:    5,
	}

	for _, m := range mutate {
		m(&config)
	}

	h := &harness{t: t, closing: make(chan error, 8), observer: NewRabbitObserver()}

	broker, err := NewMessageBrokerRabbitMQ(config, h.observer, h.closing)
	if err != nil {
		t.Fatalf("connecting to the broker at %s: %v", brokerURL, err)
	}

	h.broker = broker

	go func() {
		for err := range h.closing {
			h.mu.Lock()
			h.notified = append(h.notified, err)
			stopped := h.stopped
			h.mu.Unlock()

			// Once the test is over the harness must not reconnect. Without this
			// its own teardown looks like an outage, the loop dials again, and
			// the leftover connection pollutes the counts the next test reads.
			if stopped {
				continue
			}

			broker.Notify()
		}
	}()

	t.Cleanup(func() {
		h.mu.Lock()
		h.stopped = true
		h.mu.Unlock()

		if broker.Connection != nil && !broker.Connection.IsClosed() {
			broker.Connection.Close()
		}

		// Give the close notification time to reach the loop before the next
		// test starts counting connections.
		eventually(t, "the connection to be gone", func() bool {
			return broker.Connection == nil || broker.Connection.IsClosed()
		})
	})

	return h
}

// suppressReconnect stops the harness from reacting to close notifications, so
// a test can exercise the reconnection Publish and Consume start by themselves.
func (h *harness) suppressReconnect() {
	h.mu.Lock()
	h.stopped = true
	h.mu.Unlock()
}

func (h *harness) closeNotifications() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.notified)
}

// ---------------------------------------------------------------- naming

// names keeps every test on its own exchange, queue and routing key, so one
// test's declarations can never collide with another's.
type names struct {
	Queue    string
	Exchange string
	Routing  string
}

func newNames(t *testing.T) names {
	t.Helper()

	safe := strings.NewReplacer("/", ".", " ", ".").Replace(t.Name())
	suffix := fmt.Sprintf("%s.%d", safe, time.Now().UnixNano())

	n := names{
		Queue:    "q." + suffix,
		Exchange: "x." + suffix,
		Routing:  "r." + suffix,
	}

	t.Cleanup(func() {
		deleteQueue(t, n.Queue)
		deleteExchange(t, n.Exchange)
	})

	return n
}

// ---------------------------------------------------------- management API

type queueState struct {
	Name      string         `json:"name"`
	Type      string         `json:"type"`
	Durable   bool           `json:"durable"`
	Arguments map[string]any `json:"arguments"`
	Messages  int            `json:"messages"`
	Ready     int            `json:"messages_ready"`
	Unacked   int            `json:"messages_unacknowledged"`
	Consumers int            `json:"consumers"`
}

func management(t *testing.T, method, path string, out any) int {
	t.Helper()

	request, err := http.NewRequest(method, managementURL+path, nil)
	if err != nil {
		t.Fatalf("building the management request: %v", err)
	}

	request.SetBasicAuth(managementUser, managementPass)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("calling the management API: %v", err)
	}
	defer response.Body.Close()

	if out != nil && response.StatusCode == http.StatusOK {
		if err := json.NewDecoder(response.Body).Decode(out); err != nil {
			t.Fatalf("decoding the management response: %v", err)
		}
	}

	return response.StatusCode
}

func inspectQueue(t *testing.T, name string) (queueState, bool) {
	t.Helper()

	var state queueState
	status := management(t, http.MethodGet, "/queues/%2F/"+url.PathEscape(name), &state)

	return state, status == http.StatusOK
}

func deleteQueue(t *testing.T, name string) {
	t.Helper()
	management(t, http.MethodDelete, "/queues/%2F/"+url.PathEscape(name), nil)
}

func deleteExchange(t *testing.T, name string) {
	t.Helper()
	management(t, http.MethodDelete, "/exchanges/%2F/"+url.PathEscape(name), nil)
}

func connectionNames(t *testing.T) []string {
	t.Helper()

	var connections []struct {
		Name string `json:"name"`
	}

	management(t, http.MethodGet, "/connections", &connections)

	names := make([]string, 0, len(connections))
	for _, c := range connections {
		names = append(names, c.Name)
	}

	return names
}

// killConnection drops a connection from the broker side, which is how a real
// outage looks to the client: no local close, just the socket going away.
func killConnection(t *testing.T, name string) {
	t.Helper()
	management(t, http.MethodDelete, "/connections/"+url.PathEscape(name), nil)
}

// killEveryConnection waits for the broker to report the process's connection
// before dropping it. The wait is not optional: the management API serves
// connections out of the statistics database, which is refreshed on an interval,
// so a connection that was just opened is not listed yet.
func killEveryConnection(t *testing.T) {
	t.Helper()

	var names []string

	eventually(t, "the broker to report the connection", func() bool {
		names = connectionNames(t)
		return len(names) > 0
	})

	for _, name := range names {
		killConnection(t, name)
	}
}

// waitForConnections waits until the broker reports exactly this many
// connections, allowing for the same statistics delay.
func waitForConnections(t *testing.T, expected int) {
	t.Helper()

	eventually(t, fmt.Sprintf("the broker to report %d connection(s)", expected), func() bool {
		return len(connectionNames(t)) == expected
	})
}

// ---------------------------------------------------------------- waiting

// eventually polls because every assertion here is about broker state that
// settles asynchronously: statistics are refreshed on an interval, and a
// reconnection takes as long as it takes.
func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}

func receive(t *testing.T, success chan messagebroker.MessageBrokerPayload) messagebroker.MessageBrokerPayload {
	t.Helper()

	select {
	case payload := <-success:
		return payload
	case <-time.After(15 * time.Second):
		t.Fatal("no message was delivered")
		return messagebroker.MessageBrokerPayload{}
	}
}

func expectNoDelivery(t *testing.T, success chan messagebroker.MessageBrokerPayload, within time.Duration) {
	t.Helper()

	select {
	case payload := <-success:
		t.Fatalf("unexpected delivery: %s", payload.Body)
	case <-time.After(within):
	}
}

func consumeOptions(n names) messagebroker.MessageBrokerDeliveryOptions {
	return messagebroker.MessageBrokerDeliveryOptions{
		QueueName:      n.Queue,
		ExchangeName:   n.Exchange,
		ExchangeType:   "topic",
		RoutingKey:     n.Routing,
		MessageDurable: true,
		HasExchange:    true,
	}
}

// ---------------------------------------------------------- subscriber

// subscriber consumes a queue and subscribes again on every reconnection, which
// is what the platform's consumers do by registering with the observer.
type subscriber struct {
	t          *testing.T
	broker     *MessageBrokerRabbitMQ
	options    messagebroker.MessageBrokerDeliveryOptions
	deliveries chan messagebroker.MessageBrokerPayload

	resubscribes atomicCounter
}

type atomicCounter struct {
	mu sync.Mutex
	n  int
}

func (c *atomicCounter) add() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *atomicCounter) load() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.n
}

func (s *subscriber) GetID() string { return s.options.QueueName }

func (s *subscriber) Update() {
	s.resubscribes.add()
	s.consume()
}

func (s *subscriber) consume() {
	if err := s.broker.Consume(s.options, s.deliveries, make(chan error, 1)); err != nil {
		s.t.Logf("subscribing to %s: %v", s.options.QueueName, err)
	}
}

// newSubscriber registers the consumer with the observer and subscribes once,
// the same order NewReceiveIdentity and its siblings follow.
func newSubscriber(t *testing.T, h *harness, options messagebroker.MessageBrokerDeliveryOptions) *subscriber {
	t.Helper()

	s := &subscriber{
		t:          t,
		broker:     h.broker,
		options:    options,
		deliveries: make(chan messagebroker.MessageBrokerPayload, 64),
	}

	h.observer.Register(s)
	s.consume()

	return s
}
