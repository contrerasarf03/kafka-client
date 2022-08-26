package kafkaclient

// Bus is the main interface for working with a Bus implementation.
// All types need to implement this.
type Bus interface {
	Publish(topic string, message string) (string, error)
	Subscribe(topic string, handler MessageHandler) (chan interface{}, error)
}

// MessageHandler handles incoming messages. If an error is returned, the subscription
// will be closed.
type MessageHandler func([]byte) error
