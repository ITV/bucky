package com.itv.bucky.example.lifecycle

import com.itv.lifecycle.{Lifecycle, VanillaLifecycle}

//start snippet 2
case class AmqpChannel() {
  def startConsuming(queueName: String): Unit =
    println("Consuming " + queueName)

  def stopConsuming(queueName: String): Unit =
    println("Stopped consuming " + queueName)
}
//end snippet 2

//start snippet 1
case class AmqpConnection() {
  var isOpen = false

  def open(): Unit = {
    isOpen = true
    println("Opening AMQP connection")
  }

  def allocateChannel(): AmqpChannel = {
    require(isOpen, "connection is not open")
    println("Allocating channel")
    AmqpChannel()
  }

  def close(): Unit = {
    isOpen = false
    println("Closed AMQP connection")
  }
}
//end snippet 1

//start snippet 3
object ManualExample extends App {

  val queueName = "queue1"

  def program(): Unit =
    println("Program running")

  val connection = AmqpConnection()
  try {
    connection.open()
    val channel = connection.allocateChannel()
    try {
      channel.startConsuming(queueName)
      program()
    } finally {
      channel.stopConsuming(queueName)
    }
  } finally {
    connection.close()
  }

}
//end snippet 3

object OuterLifecycleExample {

  //start snippet 4
  val connectionLifecycle = new VanillaLifecycle[AmqpConnection] {
    override def start(): AmqpConnection = {
      val connection = AmqpConnection()
      connection.open()
      connection
    }

    override def shutdown(connection: AmqpConnection): Unit =
      connection.close()
  }

  def consumerLifecycle(connection: AmqpConnection, queueName: String) =
    new VanillaLifecycle[AmqpChannel] {
      override def start(): AmqpChannel = {
        val channel = connection.allocateChannel()
        channel.startConsuming(queueName)
        channel
      }

      override def shutdown(channel: AmqpChannel): Unit =
        channel.stopConsuming(queueName)
    }

  //end snippet 4

  //start snippet 5
  object LifecycleExample extends App {

    def program(): Unit =
      println("Program running")

    val main =
      for {
        connection <- connectionLifecycle
        _          <- consumerLifecycle(connection, "queue1")
      } yield ()

    main.map(_ => program()).runUntilJvmShutdown()
  }
  //end snippet 5

}
