package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	"github.com/contrerasarf03/sync"
)

type (
	// KafkaConfig is the configuration for a Kafka Message Bus
	KafkaConfig struct {

		// Brokers are the list of brokers in the Kafka deployment
		Brokers []string

		// MinBytes is the minimum number of bytes to fetch from a request to Kafka
		MinBytes int

		// MaxBytes is the minimum number of bytes to fetch from a request to Kafka
		MaxBytes int

		// Kafka Topic is used to categorize and organize messages
		Topic string

		// GroupID to which this Kafka client belongs to
		GroupID string

		// AsyncPublish if set to true, will enable asynchronous publish of messages to kafka
		AsyncPublish bool

		// AsyncSubscription if set to true, will enable asynchronous handling of messages from kafka
		AsyncSubscription bool

		// AsyncRoutines sets max number of works that will handle the messages
		AsyncRoutines int

		// APIKey is a unique identifier that authenticates the request
		APIKey string

		// APISecret is used for authentication
		APISecret string

		// FailOnError ensures that on an error, messages will not be committed
		FailOnError bool
	}

	// Kafka is a concrete implementation of the Bus that uses Kafka as the backend service.
	Kafka struct {
		config *KafkaConfig
		writer *kafka.Producer
	}
)

// NewKafka returns a new instance of the Kafka Message Bus implementation
func NewKafka(config *KafkaConfig) Bus {
	k := &Kafka{config: config}
	conf, _ := k.FormatConfig()
	writer, err := kafka.NewProducer(conf)
	if err != nil {
		logrus.Error("Failed to create new producer instance: ", err.Error())
	}

	k.writer = writer
	return k
}

// Publish sends topic messages to Kafka
func (k *Kafka) Publish(topic string, message string) (string, error) {

	logrus.Info("Create new producer instance: ", map[string]interface{}{
		"brokers": k.config.Brokers,
		"topic":   topic,
	})

	logrus.Info("Publish kafka message: ", message)

	err := k.writer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
	}, nil)
	if err != nil {
		logrus.Error("Failed to publish message: ", map[string]interface{}{
			"err":     err,
			"message": message,
		})

		return "", err
	}
	return "", nil
}

// Subscribe subscribes to a topic in Kafka and forwards the message to the handler.
// This returns a channel that be used to end the subscription. When the handler
// returns an error, depending on failOnError, will either close the subscription
// or ignore the error
func (k *Kafka) Subscribe(topic string, handler MessageHandler) (chan interface{}, error) {
	conf, _ := k.FormatConfig()

	logrus.Info("Create new consumer instance: ", map[string]interface{}{
		"brokers": k.config.Brokers,
		"topic":   topic,
		"groupID": k.config.GroupID,
	})
	consumer, err := kafka.NewConsumer(conf)
	if err != nil {
		logrus.Error("Failed to create new consumer instance: ", err.Error())
	}

	var endSubChan chan interface{}
	var subEndedChan chan interface{}
	failOnError := k.config.FailOnError
	if k.config.AsyncSubscription {
		endSubChan, subEndedChan = sync.ParallelTask(k.config.AsyncRoutines, 0, failOnError, subscribeTask(consumer, handler, topic))
	} else {
		endSubChan, subEndedChan = sync.LinearTask(0, failOnError, subscribeTask(consumer, handler, topic))
	}
	go func() {
		<-subEndedChan
		if err := consumer.Close(); err != nil {
			log.WithError(err).Warn("unable to close kafka reader")
		}
	}()
	return endSubChan, nil
}

func subscribeTask(reader *kafka.Consumer, handler MessageHandler, topic string) func() error {
	return func() error {
		err := reader.Subscribe(topic, nil)
		if err != nil {
			logrus.Error("Failed to subscribe to topic: ", err.Error())
		} else {
			logrus.Info("Subscribed to topic: " + topic)
		}

		message, err := reader.ReadMessage(-1)
		if err != nil {
			log.WithError(err).Warn("unable to read from Kafka, the reader might have been closed, the subscription will end")
			return err
		}
		if err := handleMessage(reader, message, handler); err != nil {
			log.WithError(err).Error("an error occurred during handling of the message, will not commit message and subscription will end")
			return err
		}
		if _, err := reader.CommitMessage(message); err != nil {
			log.WithError(err).Error("an error occurred during message commit, this subscription will end")
		}
		return nil
	}
}

func handleMessage(reader *kafka.Consumer, message *kafka.Message, handler MessageHandler) error {
	if err := handler(message.Value); err != nil {
		log.WithError(err).Error("error during handling of message")
		return err
	}
	return nil
}

func (k *Kafka) Close() {
	k.writer.Close()
}
