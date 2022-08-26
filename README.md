kafka-client
==========

`kafka-client` is a library for working with message bus such as kafka.

To use:

```
go get -u github.com/contrerasarf03/kafka-client
```

```
import "github.com/contrerasarf03/kafka-client"

func test() {
    // create kafka instance
    config := createKafkaConfig()
    bus := kafkaclient.NewKafka(config)

    // define topic
    topic := "test"

    // subscribe to a topic
    subcription := bus.Subscribe(topic, func(msg []byte) error {
        // do something with message
    })

    // publish to a topic
    bus.Publish(topic, "data")

    // close subscription
    subscription.Close()
}
```