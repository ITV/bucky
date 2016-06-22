package itv.bucky

import scala.collection.mutable

class Payload(val value: Array[Byte]) {

  override def equals(obj: scala.Any): Boolean =
    obj match {
      case mb: Payload => java.util.Arrays.equals(value, mb.value)
      case _ => false
    }

  override def hashCode(): Int = java.util.Arrays.hashCode(value)

  override def toString: String = {
    val sb = new mutable.StringBuilder(Math.min(value.length, 500) + 6)
    sb.append('[').append(value.length).append("]\"")
    value.iterator.take(500) foreach {
      case '\\' => sb.append("\\\\")
      case '"' => sb.append("\\\"")
      case b if b >= 32 && b < 127 => sb.append(b.toChar)
      case '\r' => sb.append("\\r")
      case '\n' => sb.append("\\n")
      case '\t' => sb.append("\\t")
      case _ => sb.append('.')
    }
    sb.append('"').toString()
  }

  def to[T](implicit unmarshaller: PayloadUnmarshaller[T]): UnmarshalResult[T] =
    unmarshaller(this)

}

object Payload {
  def apply(value: Array[Byte]): Payload = new Payload(value)
  val empty: Payload = new Payload(Array.empty)
  def from[T](value: T)(implicit marshaller: PayloadMarshaller[T]): Payload =
    marshaller(value)
}


sealed trait UnmarshalResult[+T]
object UnmarshalResult {

  case class Success[T](value: T) extends UnmarshalResult[T]
  case class Failure[T](reason: String) extends UnmarshalResult[T]

  implicit class SuccessConverter[T](val value: T) {
    def success: UnmarshalResult[T] = Success(value)
  }

  implicit class FailureConverter[T](val reason: String) {
    def failure: UnmarshalResult[T] = Failure(reason)
  }

}

trait PayloadUnmarshaller[+T] extends (Payload => UnmarshalResult[T])
trait PayloadMarshaller[-T] extends (T => Payload)

object PayloadMarshaller {
  implicit val UTF8StringPayloadMarshaller: PayloadMarshaller[String] = new PayloadMarshaller[String] {
    override def apply(s: String): Payload =
      Payload(s.getBytes("UTF-8"))
  }
}

object PayloadUnmarshaller {
  import UnmarshalResult._

  implicit val UTF8StringPayloadUnmarshaller: PayloadUnmarshaller[String] = new PayloadUnmarshaller[String] {
    override def apply(payload: Payload): UnmarshalResult[String] =
      new String(payload.value, "UTF-8").success
  }
}