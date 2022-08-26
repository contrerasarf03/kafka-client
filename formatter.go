package kafkaclient

import (
	"strings"

	kafkalib "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
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

	return &configMap, nil
}
