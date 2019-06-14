##Key Concepts
Bucky is built on top of the RabbitMQ [Java client](https://www.rabbitmq.com/tutorials/tutorial-one-java.html).  
Even though the complexity of dealing with the java library directly was greatly simplified,  there are still 
some key concepts which are important in order to fully understand Bucky. 

### Connection
A [connection](https://www.rabbitmq.com/connections.html) is a no more than a TCP connection to a Rabbit Broker.
Connections are meant to be long lived and to be shared by multiple consumers in a single application. Unless explicitly
specified otherwise (by creating multiple connections), bucky will only allocate one connection per application.

### Channel
A [channel](https://www.rabbitmq.com/channels.html) can be seen as a lightweight connection that share a single TCP connection
with other channels.

Channels, like connections, are meant to be long lived and can be shared across multiple consumers/publishers within a single application.

On creation, Bucky will allocate a single channel which will be used for:

- publishing messages
- performing declarations

then, for each consumer that's registered, a new channel will be allocated.

### Channel expections

If an [exception](https://www.rabbitmq.com/channels.html#error-handling) occurs on a channel it will permanently closed and
further calls that use that channel will result in failure. Given the current way, channels are allocated, this means that
if a declaration or a publishing action result in one of these [exception](https://www.rabbitmq.com/channels.html#error-handling) 
declarations and publishing will fail until the channel is restarted; consumption of messages should continue to work properly nevertheless. 