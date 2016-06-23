package itv.bucky

import argonaut.{CodecJson, DecodeJson, Parse}
import itv.bucky.PayloadUnmarshaller.StringPayloadUnmarshaller
import itv.utils.{Blob, BlobUnmarshaller}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.{Random, Try}
import scala.xml.Node
import scalaz.{Failure => _, Success => _, _}
import Scalaz._

class PayloadMarshalTest extends FunSuite {

  test("Can marshal from/to String") {
    val value = Random.alphanumeric.take(10).mkString
    UnmarshalResult.Success(value) shouldBe Payload.from(value).to[String]
  }

  test("Can marshal from/to Int") {
    implicit val intMarshaller: PayloadMarshaller[Int] = new PayloadMarshaller[Int] {
      override def apply(value: Int): Payload = Payload.from(value.toString)
    }
    import UnmarshalResult._

    val stringToInt: String => PayloadUnmarshaller[Int] = { s =>
      val parsed = Try(s.toInt).toOption
      val result = parsed.fold[UnmarshalResult[Int]](s"'$s' was not a valid integer".unmarshalFailure)(_.unmarshalSuccess)
      PayloadUnmarshaller.liftResult(result)
    }

    implicit val intUnmarshaller: PayloadUnmarshaller[Int] = StringPayloadUnmarshaller flatMap stringToInt

    val value = Random.nextInt()
    value.unmarshalSuccess shouldBe Payload.from(value).to[Int]

    "'blah' was not a valid integer".unmarshalFailure shouldBe Payload.from("blah").to[Int]
  }

  test("Can marshal from/to itv.utils.Blob") {
    import UnmarshalResult._
    implicit val blobMarshaller: PayloadMarshaller[Blob] = new PayloadMarshaller[Blob] {
      override def apply(blob: Blob): Payload = Payload(blob.content)
    }
    implicit def blobUnmarshaller[T](implicit blobUnmarshaller: BlobUnmarshaller[T]): PayloadUnmarshaller[T] = new PayloadUnmarshaller[T] {
      override def unmarshal(payload: Payload): UnmarshalResult[T] =
        try {
          blobUnmarshaller.fromBlob(Blob(payload.value)).unmarshalSuccess
        } catch {
          case t : Throwable => t.unmarshalFailure()
        }
    }

    Blob.from("Hello").unmarshalSuccess shouldBe Payload.from("Hello").to[Blob]

    val Failure(reason, throwable) = Payload.from("Hello").to[Node]

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
      StringPayloadUnmarshaller flatMap { s =>
        PayloadUnmarshaller.liftResult(Parse.decodeEither(s)(decodeJson) match {
          case -\/(reason) => UnmarshalResult.Failure(reason)
          case \/-(value) => UnmarshalResult.Success(value)
        })
      }

    import Hello._

    Hello("world").unmarshalSuccess shouldBe payload.to[Hello]
  }


}
