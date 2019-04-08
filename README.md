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
val buckyVersion = "#latest"
libraryDependencies ++= Seq(
  "com.itv" %% "bucky-core" % buckyVersion,
  "com.itv" %% "bucky-argonaut" % buckyVersion, // optional argonaut marshalling support
  "com.itv" %% "bucky-circe" % buckyVersion, // optional circe marshalling support
  "com.itv" %% "bucky-xml" % buckyVersion, // optional xml marshalling support
  "com.itv" %% "bucky-test" % buckyVersion % "test" // optional test utilities
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
    


