package kafkaclient

import (
	"strings"

	kafkalib "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

const (
	securityProtocol = "SASL_SSL"
	saslMechanism    = "PLAIN"
)

func (s *Kafka) FormatConfig() (*kafkalib.ConfigMap, error) {
	var (
		configMap = kafkalib.ConfigMap{}
		err       error
	)

	if s.config.Brokers != nil {
		brokers := strings.Join(s.config.Brokers, ",")
		err = configMap.SetKey("bootstrap.servers", brokers)
		if err != nil {
			logrus.Error("Failed to set kafka config: ", map[string]interface{}{
				"err":   err,
				"key":   "bootstrap.servers",
				"value": brokers,
			})

			return nil, err
		}
	}

	if s.config.GroupID != "" {
		err = configMap.SetKey("group.id", s.config.GroupID)
		if err != nil {
			logrus.Error("Failed to set kafka config: ", map[string]interface{}{
				"err":   err,
				"key":   "group.id",
				"value": s.config.GroupID,
			})

			return nil, err
		}
	}

	if s.config.APIKey != "" {
		err = configMap.SetKey("sasl.username", s.config.APIKey)
		if err != nil {
			logrus.Error("Failed to set kafka config: ", map[string]interface{}{
				"err":   err,
				"key":   "sasl.username",
				"value": s.config.APIKey,
			})

			return nil, err
		}
	}

	if s.config.APISecret != "" {
		err = configMap.SetKey("sasl.password", s.config.APISecret)
		if err != nil {
			logrus.Error("Failed to set kafka config: ", map[string]interface{}{
				"err":   err,
				"key":   "sasl.password",
				"value": s.config.APISecret,
			})

			return nil, err
		}
	}

	// Set default security.protocol
	err = configMap.SetKey("security.protocol", securityProtocol)
	if err != nil {
		logrus.Error("Failed to set kafka config: ", map[string]interface{}{
			"err":   err,
			"key":   "security.protocol",
			"value": securityProtocol,
		})

		return nil, err
	}

	// Set default sasl.mechanisms
	err = configMap.SetKey("sasl.mechanisms", saslMechanism)
	if err != nil {
		logrus.Error("Failed to set kafka config: ", map[string]interface{}{
			"err":   err,
			"key":   "sasl.mechanisms",
			"value": saslMechanism,
		})

		return nil, err
	}

	return &configMap, nil
}
