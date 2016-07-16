package itv.bucky

import org.scalatest.FunSuite

import scala.util.Random
import org.scalatest.Matchers._

class PayloadTest extends FunSuite{

  test("2 Payload instances with identical values are considered equal") {
    val bytes = randomArrayOfBytes()
    val clone = bytes.clone()

    bytes shouldNot be theSameInstanceAs clone

    Payload(bytes) shouldBe Payload(clone)
    Payload(bytes).hashCode() shouldBe Payload(clone).hashCode()
  }

  test("2 Payload instances with non-identitcal values are not considered equal") {
    val payload1 = Payload(randomArrayOfBytes(20))
    val payload2 = Payload(randomArrayOfBytes(10))

    payload1 shouldNot be(payload2)
    payload1.hashCode() shouldNot be(payload2.hashCode())
  }

  test("String Payload's should produce a nice toString") {
    val message = Random.alphanumeric.take(10).mkString
    Payload(message.getBytes).toString shouldBe s"""[${message.length}]"$message""""
  }

  test("Binary blobs produce a sensible toString") {
    val payload = Payload(randomArrayOfBytes(length = 7))

    payload.toString should startWith("[7]")
    withClue(s"blob.toString $payload should only contain characters in printable ASCII range (32-126), code ") {
      for (c <- payload.toString)
        c.toInt should (be >= 32 and be <= 126)
    }
  }

  test("Large blobs get truncated under toString") {
    val payload = Payload(randomArrayOfBytes(length = 9999))

    payload.value.length should be(9999)
    payload.toString.length should be < 9999
  }

  test("Blob.empty is empty") {
    Payload.empty.value.length shouldBe 0
  }

  def randomArrayOfBytes(length: Int = Random.nextInt(10)): Array[Byte] = {
    val content = new Array[Byte](length)
    Random.nextBytes(content)
    content
  }

}
