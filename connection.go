package vnats

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

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

// Config is a struct to hold the configuration of a NATS connection.
type Config struct {
	Password string
	Username string
	Hosts    string
	Port     int
}

// Connection is the main entry point for the library. It is used to create Publishers and Subscribers.
// It is also used to close the connection to the NATS server/ cluster.
type Connection struct {
	nats        bridge
	logger      *slog.Logger
	subscribers []*Subscriber
}

// bridge is required to use a mock for the nats functions in unit tests
type bridge interface {
	// EnsureStreamExists checks if a *nats.StreamInfo for the given streamConfig can be fetched.
	// If not it will be added.
	EnsureStreamExists(streamConfig *nats.StreamConfig) error

	// Subscribe creates a natsSubscription, that can fetch messages from a specified subject.
	// The first token, separated by dots, of a subject will be interpreted as the streamName.
	Subscribe(subject, consumerName string, mode SubscriptionMode) (*nats.Subscription, error)

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
		logger: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	conn.applyOptions(options...)
	var err error
	if conn.nats, err = newNATSBridge(servers, conn.logger); err != nil {
		return nil, fmt.Errorf("NATS Connection could not be created: %w", err)
	}
	return conn, nil
}

func (c *Connection) applyOptions(options ...Option) {
	for _, option := range options {
		option(c)
	}
}

// PublisherArgs contains the arguments for creating a new Publisher.
// By using a struct we are open for adding new arguments in the future
// and the caller can omit arguments where the default value is OK.
type PublisherArgs struct {
	// StreamName is the name of the stream like "PRODUCTS" or "ORDERS".
	// If it does not exist, the stream will be created.
	StreamName string
	// Replicas is the number of replicas for the stream. default is 3
	Replicas int
}

// SubscriberArgs contains the arguments for creating a new Subscriber.
// By using a struct we are open for adding new arguments in the future
// and the caller can omit arguments where the default value is OK.
type SubscriberArgs struct {
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
	for _, sub := range c.subscribers {
		if err := sub.subscription.Drain(); err != nil {
			return err
		}
		sub.quitSignal <- true
		close(sub.quitSignal)
	}
	if err := c.nats.Drain(); err != nil {
		return fmt.Errorf("NATS Connection could not be closed: %w", err)
	}
	c.logger.Info("NATS Connection closed.")
	return nil
}

// WithLogger sets the logger
// This option can be passed in the Connect function.
// Without this option, the default logger is a slog instance with level ERROR
func WithLogger(logger *slog.Logger) Option {
	return func(c *Connection) {
		c.logger = logger
	}
}

// MustConnectToNATS to NATS Server. This function panics if the connection could not be established.
// servers: List of NATS servers in the form of "nats://<user:password>@<host>:<port>"
// logger: an optional slog.Logger instance
func MustConnectToNATS(config *Config, logger *slog.Logger) *Connection {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	natsConn, err := Connect(servers(config), WithLogger(logger))
	if err != nil {
		panic("error while connecting to nats: " + err.Error())
	}
	return natsConn
}

func servers(cfg *Config) []string {
	parsedServers := trimSpaceSlice(strings.Split(cfg.Hosts, ","))
	servers := make([]string, 0, len(parsedServers))

	for _, server := range parsedServers {
		s := fmt.Sprintf("nats://%s:%s@%s:%d", cfg.Username, cfg.Password, server, cfg.Port)
		servers = append(servers, s)
	}

	return servers
}

func trimSpaceSlice(values []string) []string {
	trimmed := make([]string, len(values))
	for i, value := range values {
		trimmed[i] = strings.TrimSpace(value)
	}
	return trimmed
}
