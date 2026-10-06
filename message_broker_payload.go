package messagebroker

type MessageBrokerPayload struct {
	Body          []byte
	CorrelationID string
	ContentType   string
	Exchange      string
	DeliveryMode  uint8
	RoutingKey    string

	// Redelivered is true when the broker has handed this message over before.
	// A consumer needs it to bound its retries: without it, requeueing a
	// message that always fails turns into an immediate redelivery loop.
	Redelivered bool

	// Ack confirms the message. The argument acknowledges every message up to
	// this one when true, or only this one when false.
	Ack func(multiple bool) error

	// Nack rejects the message. The second argument puts it back on the queue
	// when true, which is what a consumer wants after a transient failure: the
	// broker redelivers it instead of holding it unacknowledged until the
	// channel closes.
	//
	// Pass false to discard it, so the queue's dead letter configuration takes
	// over if it has one.
	Nack func(multiple, requeue bool) error

	// Reject rejects this single message. It is Nack without the multiple flag,
	// kept because it is the operation the AMQP spec defines for one delivery.
	Reject func(requeue bool) error
}
