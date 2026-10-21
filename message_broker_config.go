package messagebroker

type MessageBrokerConfig struct {
	URL string

	// Deprecated: nothing reads it. Acknowledging is the consumer's decision,
	// taken through the Ack, Nack and Reject functions on the payload. It is
	// kept so the applications that set it keep compiling.
	ShouldAckInmediately bool

	ShouldReconnect bool
	ReconnectDelay  int
	RetriesCount    int
	PrefetchCount   int
}

type MessageBrokerDeliveryOptions struct {
	HasExchange    bool
	ExchangeName   string
	ExchangeType   string
	MessageDurable bool
	RoutingKey     string
	QueueName      string
	ConsumerTag    string
	CorrelationID  string
	NoAck          bool

	// DeleteWhenUnused, Exclusive and NoWait go straight to the queue
	// declaration. Their zero value matches what the library always declared,
	// so existing callers keep the same queues.
	DeleteWhenUnused bool
	Exclusive        bool
	NoWait           bool

	// QueueArguments must be identical wherever the same queue is declared:
	// RabbitMQ rejects a redeclaration with different arguments and closes the
	// channel with PRECONDITION_FAILED.
	QueueArguments QueueArguments
}

// QueueArguments holds the optional x-arguments of a queue. A zero value field
// is left out of the declaration, so the broker applies its own default.
type QueueArguments struct {
	// MessageTTL is how long a message may stay in the queue, in milliseconds.
	MessageTTL int

	// DeadLetterExchange receives the messages that are rejected without
	// requeue or that expire.
	DeadLetterExchange string

	// DeadLetterRoutingKey replaces the original routing key of a dead
	// lettered message. When empty, the original one is kept.
	DeadLetterRoutingKey string

	// XQueueType is "classic", "quorum" or "stream". Quorum and stream queues
	// only accept durable declarations, so MessageDurable must be true.
	XQueueType string

	// DeliveryLimit is how often a message may be redelivered before the broker
	// gives up on it and dead letters or drops it. Only quorum queues count
	// deliveries.
	//
	// Without it a consumer that requeues what it cannot handle loops on the
	// same message for good: brokers apply no limit of their own unless a policy
	// sets one.
	DeliveryLimit int
}
