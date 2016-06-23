package itv.bucky

import itv.bucky.UnmarshalResult.{Failure, Success}

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
    unmarshaller.unmarshal(this)

}

object Payload {
  def apply(value: Array[Byte]): Payload = new Payload(value)
  val empty: Payload = new Payload(Array.empty)
  def from[T](value: T)(implicit marshaller: PayloadMarshaller[T]): Payload =
    marshaller(value)
}


sealed trait UnmarshalResult[+T] {

  def flatMap[U](f: T => UnmarshalResult[U]): UnmarshalResult[U]
  def map[U](f: T => U): UnmarshalResult[U]

}

object UnmarshalResult {

  case class Success[T](value: T) extends UnmarshalResult[T] {
    override def flatMap[U](f: (T) => UnmarshalResult[U]): UnmarshalResult[U] = f(value)
    override def map[U](f: (T) => U): UnmarshalResult[U] = Success(f(value))
  }
  case class Failure(reason: String, throwable: Option[Throwable] = None) extends UnmarshalResult[Nothing] {
    override def flatMap[U](f: (Nothing) => UnmarshalResult[U]): UnmarshalResult[U] = this
    override def map[U](f: (Nothing) => U): UnmarshalResult[U] = this
  }

  implicit class SuccessConverter[T](val value: T) {
    def unmarshalSuccess: UnmarshalResult[T] = Success(value)
  }

  implicit class FailureConverter[T](val reason: String) {
    def unmarshalFailure: UnmarshalResult[T] = Failure(reason)
  }

  implicit class FailureThrowableConverter[T](val throwable: Throwable) {
    def unmarshalFailure(reason: String = throwable.getMessage): UnmarshalResult[T] = Failure(reason, Some(throwable))
  }

}

trait PayloadUnmarshaller[+T] { self =>

  def unmarshal(payload: Payload): UnmarshalResult[T]

  def map[U](f: T => U): PayloadUnmarshaller[U] =
    new PayloadUnmarshaller[U] {
      override def unmarshal(payload: Payload) = self.unmarshal(payload) map f
    }

  def flatMap[U](f: T => PayloadUnmarshaller[U]): PayloadUnmarshaller[U] =
    new PayloadUnmarshaller[U] {
      override def unmarshal(payload: Payload): UnmarshalResult[U] =
        self.unmarshal(payload) flatMap { result => f(result).unmarshal(payload) }
    }

}

trait PayloadMarshaller[-T] extends (T => Payload)

object PayloadMarshaller {
  implicit val UTF8StringPayloadMarshaller: PayloadMarshaller[String] = new PayloadMarshaller[String] {
    override def apply(s: String): Payload =
      Payload(s.getBytes("UTF-8"))
  }
}

object PayloadUnmarshaller {
  import UnmarshalResult._

  implicit object StringPayloadUnmarshaller extends PayloadUnmarshaller[String] {
    override def unmarshal(payload: Payload): UnmarshalResult[String] =
      new String(payload.value, "UTF-8").unmarshalSuccess
  }

  def liftResult[T](result: UnmarshalResult[T]): PayloadUnmarshaller[T] = new PayloadUnmarshaller[T] {
    override def unmarshal(payload: Payload): UnmarshalResult[T] = result
  }

}
