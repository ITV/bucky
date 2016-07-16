package itv.bucky

import argonaut.{CodecJson, DecodeJson, Parse}
import itv.bucky.Unmarshaller._
import itv.utils.{Blob, BlobUnmarshaller}

import scala.util.{Random, Try}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.xml.Node
import scalaz.{-\/, \/-}

class PayloadMarshalTest extends FunSuite {

  test("Can marshal from/to String") {
    val value = Random.alphanumeric.take(10).mkString
    UnmarshalResult.Success(value) shouldBe Payload.from(value).unmarshal[String]
  }

  test("Can marshal from/to Int") {
    implicit val intMarshaller: PayloadMarshaller[Int] = new PayloadMarshaller[Int] {
      override def apply(value: Int): Payload = Payload.from(value.toString)
    }
    import UnmarshalResult._

    val stringToInt: Unmarshaller[String, Int] =
      new Unmarshaller[String, Int] {
        override def unmarshal(thing: String): UnmarshalResult[Int] = {
          val parsed = Try(thing.toInt).toOption
          parsed.fold[UnmarshalResult[Int]](s"'$thing' was not a valid integer".unmarshalFailure)(_.unmarshalSuccess)
        }
      }

    implicit val intUnmarshaller: Unmarshaller[Payload, Int] = StringPayloadUnmarshaller flatMap stringToInt

    val value = Random.nextInt()
    value.unmarshalSuccess shouldBe Payload.from(value).unmarshal[Int]

    "'blah' was not a valid integer".unmarshalFailure shouldBe Payload.from("blah").unmarshal[Int]
  }

  test("Can marshal from/to itv.utils.Blob") {
    import UnmarshalResult._
    implicit val blobMarshaller: PayloadMarshaller[Blob] = new PayloadMarshaller[Blob] {
      override def apply(blob: Blob): Payload = Payload(blob.content)
    }

    implicit def blobUnmarshaller[T](implicit blobUnmarshaller: BlobUnmarshaller[T]): PayloadUnmarshaller[T] =
      new PayloadUnmarshaller[T] {
        override def unmarshal(thing: Payload): UnmarshalResult[T] =
          try {
            blobUnmarshaller.fromBlob(Blob(thing.value)).unmarshalSuccess
          } catch {
            case t: Throwable => t.unmarshalFailure()
          }
      }

    Blob.from("Hello").unmarshalSuccess shouldBe Payload.from("Hello").unmarshal[Blob]

    val Failure(reason, throwable) = Payload.from("Hello").unmarshal[Node]

    reason shouldBe "Content is not allowed in prolog."
    throwable shouldBe 'defined
  }

  case class Hello(world: String)
  object Hello {
    implicit val helloCodec: CodecJson[Hello] = CodecJson.casecodec1(Hello.apply, Hello.unapply)("hello")

  }

  test("Argonaut unmarshaller") {
    import UnmarshalResult._
    val content = """{ "hello": "world" }"""

    val payload = Payload(content.getBytes("UTF-8"))

    implicit def jsonDecoder[T](implicit decodeJson: DecodeJson[T]): PayloadUnmarshaller[T] =
      StringPayloadUnmarshaller flatMap
        Unmarshaller.liftResult(s => Parse.decodeEither(s)(decodeJson) match {
          case -\/(reason) => UnmarshalResult.Failure(reason)
          case \/-(value) => UnmarshalResult.Success(value)
        })

    Hello("world").unmarshalSuccess shouldBe payload.unmarshal[Hello]
  }


}
