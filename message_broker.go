package messagebroker

type MessageBroker interface {
	Notify()
	Publish(options MessageBrokerDeliveryOptions, message any) error
	Consume(options MessageBrokerDeliveryOptions, success chan MessageBrokerPayload, fail chan error) error
}

// Subscription is a consumer that is running. Stopping it cancels the consumer
// at the broker, releases its channel and ends the goroutine feeding the
// payload channel.
//
// Whatever the subscription had taken and not acknowledged goes back to the
// queue, so stopping is safe to do mid flight.
type Subscription interface {
	Stop() error
}

// Subscriber is the optional half of MessageBroker: a broker that can hand back
// a handle on what it just subscribed. It is kept apart from MessageBroker so
// that adding it breaks no existing implementation of that interface.
//
// A consumer that resubscribes -- on every reconnection, for instance -- needs
// it to stop the previous subscription. Without it each reconnection leaves the
// one before running, with its own goroutine and its own consumer at the
// broker:
//
//	if subscriber, ok := bus.(messagebroker.Subscriber); ok {
//		if previous != nil {
//			previous.Stop()
//		}
//
//		previous, err = subscriber.Subscribe(options, success, fail)
//	}
type Subscriber interface {
	Subscribe(options MessageBrokerDeliveryOptions, success chan MessageBrokerPayload, fail chan error) (Subscription, error)
}
