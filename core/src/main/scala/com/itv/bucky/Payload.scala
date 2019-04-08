package com.itv.bucky

import scala.collection.mutable
import cats.syntax.either._

class Payload(val value: Array[Byte]) {

  override def equals(obj: scala.Any): Boolean =
    obj match {
      case mb: Payload => java.util.Arrays.equals(value, mb.value)
      case _           => false
    }

  override def hashCode(): Int = java.util.Arrays.hashCode(value)

  override def toString: String = {
    val sb = new mutable.StringBuilder(Math.min(value.length, 500) + 6)
    sb.append('[').append(value.length).append("]\"")
    value.iterator.take(500) foreach {
      case '\\'                    => sb.append("\\\\")
      case '"'                     => sb.append("\\\"")
      case b if b >= 32 && b < 127 => sb.append(b.toChar)
      case '\r'                    => sb.append("\\r")
      case '\n'                    => sb.append("\\n")
      case '\t'                    => sb.append("\\t")
      case _                       => sb.append('.')
    }
    sb.append('"').toString()
  }

  def unmarshal[T](implicit unmarshaller: PayloadUnmarshaller[T]): UnmarshalResult[T] =
    unmarshaller.unmarshal(this)

}

object Payload {
  def apply(value: Array[Byte]): Payload = new Payload(value)
  val empty: Payload                     = new Payload(Array.empty)
  def from[T](value: T)(implicit marshaller: PayloadMarshaller[T]): Payload =
    marshaller(value)
}

case class UnmarshalFailure(message: String, cause: Option[Throwable] = None) extends Exception(message, cause.orNull)

trait Unmarshaller[U, T] { self =>

  def unmarshal(thing: U): UnmarshalResult[T]

  def map[V](f: T => V): Unmarshaller[U, V] =
    new Unmarshaller[U, V] {
      override def unmarshal(thing: U): UnmarshalResult[V] = self.unmarshal(thing).map(f)
    }

  def flatMap[V](f: Unmarshaller[T, V]): Unmarshaller[U, V] =
    new Unmarshaller[U, V] {
      override def unmarshal(thing: U): UnmarshalResult[V] =
        self.unmarshal(thing) flatMap { result =>
          f.unmarshal(result)
        }
    }

  def zip[V](other: Unmarshaller[U, V]): Unmarshaller[U, (T, V)] =
    Unmarshaller.liftResult(thing =>
      for {
        t <- self.unmarshal(thing)
        v <- other.unmarshal(thing)
      } yield (t, v))

}

trait PayloadMarshaller[-T] extends (T => Payload) { self =>

  def contramap[U](f: U => T): PayloadMarshaller[U] =
    new PayloadMarshaller[U] {
      override def apply(u: U): Payload =
        self(f(u))
    }

}

object PayloadMarshaller {
  implicit object StringPayloadMarshaller extends PayloadMarshaller[String] {
    override def apply(s: String): Payload =
      Payload(s.getBytes("UTF-8"))
  }

  def apply[T](f: T => Payload): PayloadMarshaller[T] =
    new PayloadMarshaller[T] {
      override def apply(t: T): Payload = f(t)
    }

  def lift[T](f: T => Payload): PayloadMarshaller[T] = apply(f)
}

object Unmarshaller {
  def toDeliveryUnmarshaller[T](unmarshaller: PayloadUnmarshaller[T]): DeliveryUnmarshaller[T] =
    Unmarshaller.liftResult(d => unmarshaller.unmarshal(d.body))

  implicit object StringPayloadUnmarshaller extends Unmarshaller[Payload, String] {
    override def unmarshal(thing: Payload): UnmarshalResult[String] =
      Right(new String(thing.value, "UTF-8"))
  }

  def liftResult[U, T](f: U => UnmarshalResult[T]): Unmarshaller[U, T] =
    new Unmarshaller[U, T] {
      override def unmarshal(thing: U): UnmarshalResult[T] =
        f(thing)
    }

}
