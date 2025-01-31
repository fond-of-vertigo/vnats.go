package vnats

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// NewPublisher creates a new Publisher that publishes to a NATS stream.
func (c *Connection) NewPublisher(args PublisherArgs) (*Publisher, error) {
	if err := validateStreamName(args.StreamName); err != nil {
		return nil, err
	}

	replicas := c.validateReplicas(args.Replicas)

	if err := c.nats.EnsureStreamExists(&nats.StreamConfig{
		Name:       args.StreamName,
		Subjects:   []string{args.StreamName + ".>"},
		Storage:    defaultStorageType,
		Replicas:   replicas,
		Duplicates: defaultDuplicationWindow,
		MaxAge:     time.Hour * 24 * 30,
	}); err != nil {
		return nil, fmt.Errorf("publisher could not be created: %w", err)
	}

	p := &Publisher{
		conn:       c,
		logger:     c.logger,
		streamName: args.StreamName,
	}
	return p, nil
}

// Publisher is a NATS publisher that publishes to a NATS stream.
type Publisher struct {
	conn       *Connection
	streamName string
	logger     *slog.Logger
}

// Publish publishes the message (data) to the given subject.
func (p *Publisher) Publish(msg *Msg) error {
	if err := validateSubject(msg.Subject, p.streamName); err != nil {
		return err
	}

	err := p.conn.nats.PublishMsg(msg.toNATS(), msg.MsgID)
	if err != nil {
		return fmt.Errorf("message with msgID: %s @ %s could not be published: %w", msg.MsgID, msg.Subject, err)
	}
	return nil
}

func validateSubject(subject, streamName string) error {
	if err := validateStreamName(streamName); err != nil {
		return err
	}
	if subject == "" {
		return fmt.Errorf("subject cannot be empty")
	}
	if !strings.HasPrefix(subject, streamName+".") {
		return fmt.Errorf("subject needs to begin with `STREAM_NAME.`")
	}
	return nil
}

func validateStreamName(streamName string) error {
	if streamName == "" {
		return fmt.Errorf("streamName cannot be empty")
	}
	if strings.ContainsAny(streamName, "*.>") {
		return fmt.Errorf("streamName cannot contain any of chars: *.>")
	}
	return nil
}

// return the number of replicas between 3 and 5
func (c *Connection) validateReplicas(replicas int) int {
	if replicas < 1 {
		replicas = len(c.nats.Servers())
	}
	if replicas < 1 || replicas > 5 {
		return 3
	}
	return replicas
}
