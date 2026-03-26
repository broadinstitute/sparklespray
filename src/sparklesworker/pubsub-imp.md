We want to use google's pubsub for communication in production and redis in
when testing locally.

As such we want to write most code agnostic of which mechanism is being used
to communicate. To distinguish between pubsub, let's call the abstract
mechanism "ExtChannel" (External Channel). The implementation should go
under a directory called "ext_channel"

Create an interface for ExtChannel:

type ExtChannel interface {
Subscribe(ctx Context, topicName string, callback func(message []byte) ) error
Publish(ctx, topicName string, message []byte) error
}

Also create three implementations: PubSubChannel and RedisChannel and
NullChannel (Which does nothing)
