[![Build Status](https://travis-ci.org/ITV/bucky.svg?branch=master)](https://travis-ci.org/ITV/bucky)
[![Join the chat at https://gitter.im/com.itv-bucky/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/com.itv-bucky/Lobby)


# Bucky

A lightweight Scala wrapper of [rabbitmq-java-client](https://github.com/rabbitmq/rabbitmq-java-client).

Provides constructs for:

* Publishing to an AMQP exchange
* Consuming from AMQP queues
* Declaring AMQP resources
* Applying common patterns 

# Getting Started
Bucky is cross built for Scala 2.11 and Scala 2.12

```scala
val buckyVersion = "0.13"
libraryDependencies ++= Seq(
  "com.itv" %% "bucky-rabbitmq" % buckyVersion,
  
  "com.itv" %% "bucky-argonaut" % buckyVersion, // optional argonaut marshalling support
  "com.itv" %% "bucky-circe" % buckyVersion, // optional circe marshalling support
  "com.itv" %% "bucky-xml" % buckyVersion // optional xml marshalling support
)
```


### Running the unit tests
    
    sbt test

### Running the integration tests
You'll first need to install [Docker](https://docs.docker.com/docker-for-mac/install/), within which we'll run both Postgres and RabbitMQ. 
 
As well, you'll need `docker-compose`:

     brew install docker-compose
 
Then, in the base directory:

    docker-compose up

You should see log messages from both Postgres and RabbitMQ, but if you want to make doubly sure:

    docker ps
    
    CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                                   NAMES
    076e2f3591f2        postgres:9.5.4      "/docker-entrypoin..."   3 minutes ago       Up 5 seconds        0.0.0.0:5432->5432/tcp                                  fulfilmentplanningbackend_postgres_1
    b1c319fedafa        rabbitmq:3.6        "docker-entrypoint..."   3 minutes ago       Up 5 seconds        4369/tcp, 5671/tcp, 25672/tcp, 0.0.0.0:5672->5672/tcp   fulfilmentplanningbackend_some-rabbit_1 
     
Please note that you can connect to the local Postgres as user `postgres` with a blank password; the database name is `craft_dev`.
    
    sbt it:test
    
# Example projects

Basic Consumer
---
[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/com/itv/bucky/example/basic/StringConsumer.scala)

A very simple project that:
* Declares an AMQP queue
* Starts a consumer on the queue:
    * Messages are deseriailized to a `String`
    * The handle prints out the `String` and acknowledges the message
    
Basic Publisher
---
[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/com/itv/bucky/example/basic/StringPublisher.scala)

A very simple project that:
* Declares an exchange
* Adds a binding to the exchange
* Publishes a message with a `String` payload to the exchange/binding

Unmarshalling Consumer
----

[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/com/itv/bucky/example/marshalling/UnmarshallingConsumer.scala)

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

[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/com/itv/bucky/example/marshalling/MarshallingPublisher.scala)

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


Scalaz Consumer
----

[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/com/itv/bucky/example/scalaz/StringConsumer.scala)

A very simple project using scalaz that:
* Declares an AMQP queue
* Starts a consumer on the queue:
    * Messages are deseriailized to a `String`
    * The handle prints out the `String` and acknowledges the message


Scalaz Publisher
----

[source code here](https://github.com/ITV/bucky/blob/master/example/src/main/scala/com/itv/bucky/example/scalaz/StringPublisher.scala)

A very simple project using scalaz that:
* Declares an exchange
* Adds a binding to the exchange
* Publishes a message with a `String` payload to the exchange/binding



