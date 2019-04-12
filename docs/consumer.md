## Consumer

Consumers can be used to consume messages to a rabbit Broker. 

A consumer will be registered and consuming messages from the moment it's registered until
the connection is closed.


```scala
def handler(s: AMessageType) : IO[ConsumeAction] = {
  IO.pure[Ack]
}

def foo(client: AmqpClient[IO]) = {
  val publisher = client.registerConsumerOf[AMessageType](QueueName("a-queue"), handl) 
}
```


When registering a consumer, one can define a result for when the handler raises an exception by
specifying an "exceptionalAction". The following, for instance, will always Ack the message even though the handler 
always returns true.
By default `exceptionalAction` defaults to `DeadLetter` 
```scala
def handler(s: AMessageType) : IO[ConsumeAction] = {
  IO.raiseError(new RuntimeException(""))
}

def foo(client: AmqpClient[IO]) = {
  val publisher = client.registerConsumerOf[AMessageType](QueueName("a-queue"), handl, exceptionalAction = Ack) 
}
```

##### Accessing headers of consumed messages

In order to access headers and/or published message properties, one can register a consumer of delivery
which will allow for this properties to be accessed:
```scala
def handler(s: Delivery) : IO[ConsumeAction] = {
  IO.pure(Ack)
}

def foo(client: AmqpClient[IO]) = {
  val publisher = client.registerConsumer(QueueName("a-queue"), handl, exceptionalAction = Ack) 
}
```


###### Todo: Requeue handlers, Testing Handlers

