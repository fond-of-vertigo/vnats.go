package vnats

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

// SubscriptionMode defines how the consumer and its Subscriber are configured. This mode must be set accordingly
// to the use-case. If the order of messages should be strictly ordered, SingleSubscriberStrictMessageOrder should be
// used. If the message order is not important, but horizontal scaling is, use MultipleSubscribersAllowed.
type SubscriptionMode int

const (
	// MultipleSubscribersAllowed mode (default) enables multiple Subscriber of one consumer for horizontal scaling.
	// The message order cannot be guaranteed when messages get NAKed/ MsgHandler for message returns error.
	MultipleSubscribersAllowed SubscriptionMode = iota

	// SingleSubscriberStrictMessageOrder mode enables strict order of messages. If messages get NAKed/ MsgHandler for
	// message returns error, the Subscriber of consumer will retry the failed message until resolved. This blocks the
	// entire consumer, so that horizontal scaling is not effectively possible.
	SingleSubscriberStrictMessageOrder
)

const (
	LogLevelTrace = iota
	LogLevelDebug
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// LogFunc is a generic logging function to incorporate the logging of the library into the application.
// It can be set via the Option of a Connection using WithLogger(l LogFunc).
type LogFunc func(level int, format string, a ...interface{})

var NoOpLogFunc = func(_ int, _ string, _ ...interface{}) {}

// Connection is the main entry point for the library. It is used to create Publishers and Subscribers.
// It is also used to close the connection to the NATS server/ cluster.
type Connection struct {
	nats        bridge
	log         LogFunc
	subscribers []*Subscriber
}

// bridge is required to use a mock for the nats functions in unit tests
type bridge interface {
	// FetchOrAddStream returns a *nats.StreamInfo and for the given streamInfo name.
	// It adds a new streamInfo if it does not exist.
	FetchOrAddStream(streamConfig *nats.StreamConfig) (*nats.StreamInfo, error)

	// CreateSubscription creates a natsSubscription, that can fetch messages from a specified subject.
	// The first token, separated by dots, of a subject will be interpreted as the streamName.
	CreateSubscription(subject, consumerName string, mode SubscriptionMode) (*nats.Subscription, error)

	// Servers returns the list of NATS servers.
	Servers() []string

	// PublishMsg publishes a message with a context-dependent msgID to a subject.
	PublishMsg(msg *nats.Msg, msgID string) error

	// Drain will put a Connection into a drain state. All subscriptions will
	// immediately be put into a drain state. Upon completion, the publishers
	// will be drained and can not publish any additional messages. Upon draining
	// of the publishers, the Connection will be closed.
	//
	// See notes for nats.Conn.Drain
	Drain() error
}

// Option is an optional configuration argument for the Connect() function.
type Option func(*Connection)

// Connect returns Connection to a NATS server/ cluster and enables Publisher and Subscriber creation.
func Connect(servers []string, options ...Option) (*Connection, error) {
	conn := &Connection{
		log: NoOpLogFunc,
	}

	conn.applyOptions(options...)
	var err error
	if conn.nats, err = newNATSBridge(servers, conn.log); err != nil {
		return nil, fmt.Errorf("NATS Connection could not be created: %w", err)
	}
	return conn, nil
}

func (c *Connection) applyOptions(options ...Option) {
	for _, option := range options {
		option(c)
	}
}

// CreatePublisherArgs contains the arguments for creating a new Publisher.
// By using a struct we are open for adding new arguments in the future
// and the caller can omit arguments where the default value is OK.
type CreatePublisherArgs struct {
	// StreamName is the name of the stream like "PRODUCTS" or "ORDERS".
	// If it does not exist, the stream will be created.
	StreamName string
}

// CreateSubscriberArgs contains the arguments for creating a new Subscriber.
// By using a struct we are open for adding new arguments in the future
// and the caller can omit arguments where the default value is OK.
type CreateSubscriberArgs struct {
	// ConsumerName contains the name of the consumer. By default, this should be the
	// name of the service.
	ConsumerName string

	// Subject defines which subjects of the stream should be subscribed.
	// Examples:
	//  "ORDERS.new" -> subscribe subject "new" of stream "ORDERS"
	//  "ORDERS.>"   -> subscribe all subjects in any level of stream "ORDERS".
	//  "ORDERS.*"   -> subscribe all direct subjects of stream "ORDERS", like "ORDERS.new", "ORDERS.processed",
	//                  but not "ORDERS.new.error".
	Subject string

	// Mode defines the constraints of the subscription. Default is MultipleSubscribersAllowed.
	// See SubscriptionMode for details.
	Mode SubscriptionMode
}

// Close closes the NATS Connection and drains all subscriptions.
func (c *Connection) Close() error {
	c.log(LogLevelTrace, "Draining and closing open subscriptions..")
	for _, sub := range c.subscribers {
		if err := sub.subscription.Drain(); err != nil {
			return err
		}
		sub.quitSignal <- true
		close(sub.quitSignal)
	}
	c.log(LogLevelTrace, "Closed all open subscriptions.")
	c.log(LogLevelTrace, "Closing NATS Connection...")
	if err := c.nats.Drain(); err != nil {
		return fmt.Errorf("NATS Connection could not be closed: %w", err)
	}
	c.log(LogLevelInfo, "NATS Connection closed.")
	return nil
}

// WithLogger sets the logger using the generic LogFunc function.
// This option can be passed in the Connect function.
// Without this option, the default LogFunc is a nop function.
func WithLogger(log LogFunc) Option {
	return func(c *Connection) {
		c.log = log
	}
}
