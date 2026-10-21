package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaval/messagebroker"
	"github.com/streadway/amqp"
)

// The broker is both halves: the interface every application depends on, and
// the optional one that hands back a handle on a subscription.
var (
	_ messagebroker.MessageBroker = (*MessageBrokerRabbitMQ)(nil)
	_ messagebroker.Subscriber    = (*MessageBrokerRabbitMQ)(nil)
)

type MessageBrokerRabbitMQ struct {
	config             messagebroker.MessageBrokerConfig
	observer           *RabbitObserver
	loading            bool
	lock               sync.RWMutex
	notifyCloseChannel chan error

	// connectLock serialises dial. Notify, Publish and Consume each reconnect on
	// their own, and without it two of them dial at the same time and overwrite
	// each other's connection and channels, leaving one of the two orphaned at
	// the broker with its consumers and their unacknowledged deliveries.
	connectLock      sync.Mutex
	Connection       *amqp.Connection
	ConsumerChannel  *amqp.Channel
	PublisherChannel *amqp.Channel

	// consumerChannelClosed and publisherChannelClosed replace asking amqp
	// whether a channel is still usable. There is no way to ask: NotifyClose on
	// an already closed channel closes the channel it is handed, so a probe
	// reads a nil error and cannot tell that apart from a healthy channel. They
	// are set by watchClose and cleared by connect.
	consumerChannelClosed  atomic.Bool
	publisherChannelClosed atomic.Bool

	// declared remembers what this connection has already declared, so a publish
	// costs one frame instead of the synchronous declare round trip it used to
	// pay per message. dial clears it, because the declarations belong to the
	// channels it rebuilds.
	declaredLock sync.Mutex
	declared     map[string]struct{}
}

func NewMessageBrokerRabbitMQ(config messagebroker.MessageBrokerConfig, observer *RabbitObserver, notifyCloseChannel chan error) (*MessageBrokerRabbitMQ, error) {
	broker := &MessageBrokerRabbitMQ{
		config:             config,
		observer:           observer,
		notifyCloseChannel: notifyCloseChannel,
	}

	conn, err := broker.connect()
	if err != nil {
		return nil, err
	}

	broker.Connection = conn

	return broker, nil
}

func (b *MessageBrokerRabbitMQ) Notify() {
	// Claiming the reconnection has to read and set loading in one critical
	// section. Reading it, releasing the lock and only then setting it let two
	// close notifications -- the channel's and the connection's, which arrive
	// together -- both find it false and both start reconnecting.
	b.lock.Lock()
	if b.loading {
		b.lock.Unlock()
		return
	}

	b.loading = true
	b.lock.Unlock()

	if !b.config.ShouldReconnect {
		if !b.Connection.IsClosed() {
			b.Connection.Close()
		}

		err := fmt.Errorf("error: channel or connection lost and should reconnect is %v", b.config.ShouldReconnect)
		panic(err)
	}

	retries := 1

	for {
		if !b.healthy() {
			<-time.After(time.Duration(b.config.ReconnectDelay) * time.Second)

			_, err := b.connect()
			if err != nil {
				if retries >= b.config.RetriesCount {
					err = fmt.Errorf("error: max retries (%d) connection to rabbit", b.config.RetriesCount)
					panic(err)
				}

				retries++
				continue
			}
		}

		b.lock.Lock()
		b.loading = false
		b.lock.Unlock()

		break
	}
}

func (b *MessageBrokerRabbitMQ) Publish(options messagebroker.MessageBrokerDeliveryOptions, message any) error {
	if b.Connection.IsClosed() {
		if _, err := b.connect(); err != nil {
			return err
		}
	}

	var body []byte
	var err error

	queueName := options.QueueName

	if options.HasExchange {
		queueName = options.RoutingKey

		if err = b.declareOnce(exchangeKey(options), func() error {
			return setupExchangeDeclare(b.PublisherChannel, options)
		}); err != nil {
			return err
		}
	} else {
		if err = b.declareOnce(queueKey(options), func() error {
			_, err := setupQueue(b.PublisherChannel, options)
			return err
		}); err != nil {
			return err
		}
	}

	if v, ok := message.([]byte); !ok {
		if body, err = json.Marshal(message); err != nil {
			return err
		}
	} else {
		body = v
	}

	return b.PublisherChannel.Publish(
		options.ExchangeName,
		queueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode:  amqp.Persistent,
			ContentType:   "application/json",
			CorrelationId: options.CorrelationID,
			Body:          body,
			Timestamp:     time.Now(),
		})
}

// Consume subscribes to a queue and throws the handle away. Subscribe is the
// one to use when the subscription has to be stopped later.
func (b *MessageBrokerRabbitMQ) Consume(options messagebroker.MessageBrokerDeliveryOptions, success chan messagebroker.MessageBrokerPayload, fail chan error) error {
	_, err := b.Subscribe(options, success, fail)

	return err
}

// Subscribe starts consuming and hands back a handle on the subscription.
func (b *MessageBrokerRabbitMQ) Subscribe(options messagebroker.MessageBrokerDeliveryOptions, success chan messagebroker.MessageBrokerPayload, fail chan error) (messagebroker.Subscription, error) {
	if b.Connection.IsClosed() {
		if _, err := b.connect(); err != nil {
			return nil, err
		}
	}

	channel, err := b.openConsumerChannel(options, fail)
	if err != nil {
		return nil, err
	}

	if _, err := setupQueue(channel, options); err != nil {
		return nil, err
	}

	if options.HasExchange {
		if err := setupExchangeDeclare(channel, options); err != nil {
			return nil, err
		}

		if err := setupQueueBind(channel, options); err != nil {
			return nil, err
		}
	}

	// A tag of our own, because cancelling needs one and amqp does not hand back
	// the tag it generates for an empty one. It also tells the queues apart in
	// the broker's consumer list, where every one of them used to read
	// amq.ctag-<random>.
	tag := options.ConsumerTag
	if tag == "" {
		tag = fmt.Sprintf("messagebroker-%s-%d", options.QueueName, time.Now().UnixNano())
	}

	messages, err := channel.Consume(
		options.QueueName,
		tag,
		options.NoAck,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	subscription := &subscription{channel: channel, tag: tag, done: make(chan struct{})}

	go func() {
		for {
			select {
			case msg, ok := <-messages:
				if !ok {
					return
				}

				// Stopping must not wait for whoever reads the payloads: this
				// send blocks for as long as nobody does, and a stopped
				// subscription has no reader left to wait for.
				select {
				case success <- newPayload(msg):
				case <-subscription.done:
					return
				}
			case <-subscription.done:
				return
			}
		}
	}()

	return subscription, nil
}

// subscription is the handle Subscribe returns.
type subscription struct {
	channel *amqp.Channel
	tag     string
	done    chan struct{}
	once    sync.Once
}

func (s *subscription) Stop() error {
	var err error

	s.once.Do(func() {
		close(s.done)

		// Cancel first so the broker stops sending, then release the channel.
		// Both are best effort: a subscription whose channel the broker already
		// closed is stopped either way, and reporting that as a failure would
		// make callers handle an error that means nothing.
		if cancelErr := s.channel.Cancel(s.tag, false); cancelErr != nil && !isChannelGone(cancelErr) {
			err = cancelErr
		}

		if closeErr := s.channel.Close(); closeErr != nil && !isChannelGone(closeErr) && err == nil {
			err = closeErr
		}
	})

	return err
}

func isChannelGone(err error) bool {
	return errors.Is(err, amqp.ErrClosed)
}

// connect rebuilds the connection and, when it replaced an earlier one, makes
// every consumer subscribe again.
//
// The resubscription belongs here and not only in Notify because Publish and
// Consume reconnect on their own as well. Without it those two built a fresh
// consumer channel and told nobody: the delivery channels the consumers were
// ranging over belonged to the channel that had just been replaced, so they
// ended, the consumers were left blocked on a channel no one writes to any
// more, and the process kept running with no consumer registered at the broker
// while its queues filled up.
func (b *MessageBrokerRabbitMQ) connect() (*amqp.Connection, error) {
	conn, replacedConnection, err := b.dial()
	if err != nil {
		return nil, err
	}

	if replacedConnection {
		b.observer.NotifyAll()
	}

	return conn, nil
}

// dial opens the connection and its two channels. It returns whether it
// replaced an existing connection, which is what tells connect that the
// consumers have to subscribe again.
func (b *MessageBrokerRabbitMQ) dial() (*amqp.Connection, bool, error) {
	b.connectLock.Lock()
	defer b.connectLock.Unlock()

	// Whoever held the lock first may already have rebuilt everything. Dialling
	// again would strand the connection they just made and send every caller
	// that finds the connection closed at the same moment -- which is all of
	// them, after an outage -- through a dial of its own.
	if b.healthy() {
		return b.Connection, false, nil
	}

	previousConnection := b.Connection

	conn, err := amqp.Dial(b.config.URL)
	if err != nil {
		return nil, false, err
	}

	// The three notification channels are buffered on purpose. Both
	// (*amqp.Channel).shutdown and (*amqp.Connection).shutdown hand the error to
	// every registered listener with a blocking send while they hold the channel
	// lock, and they run on the connection's reader goroutine. watchClose takes
	// only the first of the three, so the other two are left without a reader:
	// unbuffered, they freeze that reader, and with it every delivery,
	// acknowledgement and further close notification of the connection.
	connectionClosingChannel := make(chan *amqp.Error, 1)
	publisherClosingChannel := make(chan *amqp.Error, 1)
	consumerClosingChannel := make(chan *amqp.Error, 1)

	b.Connection = conn
	conn.NotifyClose(connectionClosingChannel)

	consumerChannel, err := conn.Channel()
	if err != nil {
		return nil, false, err
	}

	// ConsumerChannel stays the connection's own channel, for the applications
	// that reach for the field. The subscriptions do not consume on it: each one
	// opens its own in openConsumerChannel.
	b.ConsumerChannel = consumerChannel
	b.consumerChannelClosed.Store(false)
	consumerChannel.NotifyClose(consumerClosingChannel)

	publisherChannel, err := conn.Channel()
	if err != nil {
		return nil, false, err
	}

	b.PublisherChannel = publisherChannel
	b.publisherChannelClosed.Store(false)
	publisherChannel.NotifyClose(publisherClosingChannel)

	b.forgetDeclarations()

	if b.config.PrefetchCount > 0 {
		if err := consumerChannel.Qos(b.config.PrefetchCount, 0, false); err != nil {
			return nil, false, err
		}
	}

	go b.watchClose(connectionClosingChannel, publisherClosingChannel, consumerClosingChannel)

	// Release the connection this one replaces. Left open, the broker keeps its
	// consumers registered and holds their unacknowledged deliveries in flight on
	// a connection the application no longer reads, so the same messages stay
	// stuck until the socket dies. The close runs apart from this function
	// because it waits for the broker's close-ok, and an already wedged
	// connection would otherwise block the recovery that is replacing it.
	if previousConnection != nil && previousConnection != conn && !previousConnection.IsClosed() {
		go previousConnection.Close()
	}

	return conn, previousConnection != nil, nil
}

// openConsumerChannel gives the subscription a channel of its own.
//
// They used to share a single one, so a queue declared with arguments that did
// not match took every other consumer of the process down with it -- and since
// the subscriptions are set up one after another at start up, the ones that had
// not run yet failed too. One refused declaration cost five consumers.
//
// A channel that dies on its own is not a reason to rebuild the connection:
// either it died of a protocol error, which reconnecting does not fix, or the
// connection went with it and the connection's own notification is already
// driving the reconnection. The subscription is told through the fail channel
// it was handed, which is what that channel is for.
func (b *MessageBrokerRabbitMQ) openConsumerChannel(options messagebroker.MessageBrokerDeliveryOptions, fail chan error) (*amqp.Channel, error) {
	channel, err := b.Connection.Channel()
	if err != nil {
		return nil, err
	}

	if b.config.PrefetchCount > 0 {
		if err := channel.Qos(b.config.PrefetchCount, 0, false); err != nil {
			channel.Close()
			return nil, err
		}
	}

	closing := make(chan *amqp.Error, 1)
	channel.NotifyClose(closing)

	go func() {
		err := <-closing
		if err == nil {
			return
		}

		// Never block: the subscription may be gone, and this goroutine holds
		// the last reference to a channel that is already closed.
		select {
		case fail <- fmt.Errorf("the channel consuming %s was closed: %w", options.QueueName, err):
		default:
		}
	}()

	return channel, nil
}

// declareOnce runs a declaration the first time this connection is asked for it
// and remembers it, so the publishes that follow go straight out.
//
// Only a successful declaration is remembered, and the key covers every field
// the broker compares, so a declaration that differs from one already made is
// still sent and still refused. Nothing is remembered across a reconnection: a
// queue deleted from under the application is declared again on the next one.
func (b *MessageBrokerRabbitMQ) declareOnce(key string, declare func() error) error {
	b.declaredLock.Lock()
	_, done := b.declared[key]
	b.declaredLock.Unlock()

	if done {
		return nil
	}

	if err := declare(); err != nil {
		return err
	}

	b.declaredLock.Lock()
	if b.declared == nil {
		b.declared = make(map[string]struct{})
	}

	b.declared[key] = struct{}{}
	b.declaredLock.Unlock()

	return nil
}

func (b *MessageBrokerRabbitMQ) forgetDeclarations() {
	b.declaredLock.Lock()
	b.declared = nil
	b.declaredLock.Unlock()
}

// exchangeKey and queueKey spell out everything the broker compares when
// something is declared again, so two declarations that differ anywhere are
// never mistaken for one another.
func exchangeKey(options messagebroker.MessageBrokerDeliveryOptions) string {
	return fmt.Sprintf("exchange\x00%s\x00%s\x00%t",
		options.ExchangeName, options.ExchangeType, options.MessageDurable)
}

func queueKey(options messagebroker.MessageBrokerDeliveryOptions) string {
	return fmt.Sprintf("queue\x00%s\x00%t\x00%t\x00%t\x00%t\x00%v",
		options.QueueName, options.MessageDurable, options.DeleteWhenUnused,
		options.Exclusive, options.NoWait, toAMQPTable(options.QueueArguments))
}

// healthy reports whether the connection and both channels are usable.
func (b *MessageBrokerRabbitMQ) healthy() bool {
	return b.Connection != nil && !b.Connection.IsClosed() &&
		b.ConsumerChannel != nil && !b.consumerChannelClosed.Load() &&
		b.PublisherChannel != nil && !b.publisherChannelClosed.Load()
}

// watchClose hands the first close notification to the application and records
// which half of the connection went down, which is what Notify reads to decide
// whether it has to reconnect.
func (b *MessageBrokerRabbitMQ) watchClose(connectionClosing, publisherClosing, consumerClosing chan *amqp.Error) {
	var err *amqp.Error

	select {
	case err = <-connectionClosing:
		b.consumerChannelClosed.Store(true)
		b.publisherChannelClosed.Store(true)
	case err = <-publisherClosing:
		b.publisherChannelClosed.Store(true)
	case err = <-consumerClosing:
		b.consumerChannelClosed.Store(true)
	}

	b.notifyCloseChannel <- err
}

// newPayload maps a delivery onto the payload the consumer receives.
//
// It carries Nack and Reject besides Ack because a consumer that can only
// acknowledge has no way to say "I could not process this": its options are to
// confirm a message it did not handle, or to leave it unacknowledged until the
// channel closes. Both of those delay or lose work that the broker is willing
// to redeliver.
//
// The three are methods on the delivery itself, so they already carry its
// delivery tag.
//
// None of them is safe to call on a queue consumed with NoAck. amqp fills the
// acknowledger in regardless of NoAck, so the call returns no error, sends an
// acknowledgement for a delivery tag the broker never handed out, and the
// broker answers by closing the channel. A consumer that sets NoAck must not
// call them at all.
func newPayload(msg amqp.Delivery) messagebroker.MessageBrokerPayload {
	return messagebroker.MessageBrokerPayload{
		Body:          msg.Body,
		RoutingKey:    msg.RoutingKey,
		CorrelationID: msg.CorrelationId,
		ContentType:   msg.ContentType,
		Exchange:      msg.Exchange,
		DeliveryMode:  msg.DeliveryMode,
		Redelivered:   msg.Redelivered,
		Ack:           msg.Ack,
		Nack:          msg.Nack,
		Reject:        msg.Reject,
	}
}
