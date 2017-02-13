# Bucky

A lightweight Scala wrapper of [rabbitmq-java-client](https://github.com/rabbitmq/rabbitmq-java-client).

Provides constructs for:

* Publishing to an AMQP exchange
* Consuming from AMQP queues
* Declaring AMQP resources
* Applying common patterns 

#Example projects

Basic Consumer
---
[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/itv/bucky/example/basic/StringConsumer.scala)

A very simple project that:
* Declares an AMQP queue
* Starts a consumer on the queue:
    * Messages are deseriailized to a `String`
    * The handle prints out the `String` and acknowledges the message
    
Basic Publisher
---
[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/itv/bucky/example/basic/StringPublisher.scala)

A very simple project that:
* Declares an exchange
* Adds a binding to the exchange
* Publishes a message with a `String` payload to the exchange/binding

Unmarshalling Consumer
----

[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/itv/bucky/example/marshalling/UnmarshallingConsumer.scala)

In this project we aim to consume higher level message payloads as opposed to `String`

We define a `Person` case class:

```scala
case class Person(name: String, age: Int)
```

And a `PayloadUnmarshaller[Person]` to convert AMQP messages into `Person` instances.
This unmarshaller is built in terms of `StringPayloadUnmarshaller`:

```scala
val personUnmarshaller = StringPayloadUnmarshaller flatMap Unmarshaller.liftResult { incoming =>
    incoming.split(",") match {
      case Array(name, ageString) if ageString.forall(_.isDigit) =>
        UnmarshalResult.Success(Person(name, ageString.toInt))

      case Array(name, ageNotInteger) =>
        UnmarshalResult.Failure(s"Age was not an integer in '$ageNotInteger'")

      case _ =>
        UnmarshalResult.Failure(s"Expected message to be in format <name>,<age>: got '$incoming'")
    }

  }
```

The behaviour of the project is not very dissimilar to the "Basic Consumer" project:
* Declares an AMQP queue
* Starts a consumer on the queue:
    * Messages are deseriailized to a `Person`
    * The handle prints out the `Person` and acknowledges the message

Unmarshalling Publisher
----

[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/itv/bucky/example/marshalling/MarshallingPublisher.scala)

In this project we aim to publish higher level message payloads as opposed to `String`

We define a `Person` case class:

```scala
case class Person(name: String, age: Int)
```

And a `PayloadMarshaller[Person]` to convert `Person` instances into AMQP messages.
This marshaller is built in terms of `StringPayloadMarshaller`:

```scala
val personMarshaller: PayloadMarshaller[Person] = StringPayloadMarshaller.contramap(p => s"${p.name},${p.age}")
```

The behaviour of the project is not very dissimilar to the "Basic Publisher" project:
* Declares an exchange
* Adds a binding to the exchange
* Publishes a message with a `Person` payload to the exchange/binding
