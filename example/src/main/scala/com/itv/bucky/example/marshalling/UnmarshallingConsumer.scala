package com.itv.bucky.example.marshalling

import cats.effect.{ExitCode, IO, IOApp}
import com.itv.bucky.Unmarshaller.StringPayloadUnmarshaller
import com.itv.bucky._
import com.itv.bucky.consume._
import com.itv.bucky.decl._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

object UnmarshallingConsumer extends IOApp with StrictLogging {

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

  implicit val personUnmarshaller: PayloadUnmarshaller[Person] =
    StringPayloadUnmarshaller.flatMap[Person]((thing: String) => csvStringToPerson(thing))
  //end snippet 2

  //start snippet 3
  val personHandler: Handler[IO, Person] =
    Handler[IO, Person] { message: Person =>
      IO {
        logger.info(s"${message.name} is ${message.age} years old")
        Ack
      }
    }
  //end snippet 3

  //start snippet 4
  override def run(args: List[String]): IO[ExitCode] =
    AmqpClient[IO](amqpClientConfig).use { amqpClient =>
      for {
        _ <- amqpClient.declare(Declarations.all)
             _ <- amqpClient.registerConsumerOf(Declarations.queue.name, personHandler)
      } yield ExitCode.Success
    }
  //end snippet 4


}
