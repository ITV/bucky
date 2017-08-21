package com.itv.bucky.example.marshalling

import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky._
import com.itv.bucky.decl._
import com.itv.bucky.example.marshalling.Shared.Person
import com.itv.bucky.lifecycle.{AmqpClientLifecycle, DeclarationLifecycle}
import com.itv.lifecycle.Lifecycle
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object UnmarshallingConsumer extends App with StrictLogging {

  //start snippet 1
  val config                             = ConfigFactory.load("bucky")
  val amqpClientConfig: AmqpClientConfig = AmqpClientConfig(config.getString("rmq.host"), 5672, "guest", "guest")

  object Declarations {
    val queue = Queue(QueueName("queue.people"))
    val all   = List(queue)
  }
  case class Person(name: String, age: Int)
  //end snippet 1

  //start snippet 2
  def csvStringToPerson(csvString: String): UnmarshalResult[Person] =
    //split csv into parts
    csvString.split(",") match {
      //when the csv has 2 parts, and second is all digits
      case Array(name, ageString) if ageString.forall(_.isDigit) =>
        //success! there's some chance that's someone's name and age!
        UnmarshalResult.Success(Person(name, ageString.toInt))

      //otherwise fail in an appropriate way
      case Array(name, ageNotInteger) =>
        UnmarshalResult.Failure(s"Age was not an integer in '$csvString'")

      case _ =>
        UnmarshalResult.Failure(s"Expected message to be in format <name>,<age>: got '$csvString'")
    }

  val personUnmarshaller: PayloadUnmarshaller[Person] =
    for {
      csvString <- StringPayloadUnmarshaller
      person    <- csvStringToPerson(csvString)
    } yield person
  //end snippet 2

  //start snippet 3
  val personHandler =
    Handler[Future, Person] { message: Person =>
      Future {
        logger.info(s"${message.name} is ${message.age} years old")
        Ack
      }
    }
  //end snippet 3

  //start snippet 4
  /**
    * A lifecycle is a monadic try/finally statement.
    * More detailed information is available here https://github.com/ITV/lifecycle
    */
  val lifecycle: Lifecycle[Unit] =
    for {
      amqpClient <- AmqpClientLifecycle(amqpClientConfig)
      _          <- DeclarationLifecycle(Declarations.all, amqpClient)
      effectMonad = amqpClient.effectMonad
      _ <- amqpClient.consumer(Declarations.queue.name,
                               AmqpClient.handlerOf(personHandler, personUnmarshaller)(effectMonad))
    } yield ()

  lifecycle.runUntilJvmShutdown()
  //end snippet 4

}
