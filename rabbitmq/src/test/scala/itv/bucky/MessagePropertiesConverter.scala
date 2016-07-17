package itv.bucky

import java.util.Date

import org.scalatest.FunSuite
import com.rabbitmq.client.{MessageProperties => RMessageProperties}
import org.scalatest.Matchers._

import scala.util.Random

class MessagePropertiesConverter extends FunSuite {

  test("it should be able to convert default message properties") {
    MessagePropertiesConverters(RMessageProperties.BASIC) shouldBe MessagePropertiesConverters(MessagePropertiesConverters(MessageProperties.basic))
    MessagePropertiesConverters(RMessageProperties.MINIMAL_BASIC) shouldBe MessagePropertiesConverters(MessagePropertiesConverters(MessageProperties.minimalBasic))
    MessagePropertiesConverters(RMessageProperties.MINIMAL_PERSISTENT_BASIC) shouldBe MessagePropertiesConverters(MessagePropertiesConverters(MessageProperties.minimalPersistentBasic))
    MessagePropertiesConverters(RMessageProperties.PERSISTENT_BASIC) shouldBe MessagePropertiesConverters(MessagePropertiesConverters(MessageProperties.persistentBasic))
    MessagePropertiesConverters(RMessageProperties.PERSISTENT_TEXT_PLAIN) shouldBe MessagePropertiesConverters(MessagePropertiesConverters(MessageProperties.persistentTextPlain))
    MessagePropertiesConverters(RMessageProperties.TEXT_PLAIN) shouldBe MessagePropertiesConverters(MessagePropertiesConverters(MessageProperties.textPlain))
  }

  test("it should be able to convert convert all the properties") {
    val random = new Random()
    val properties = MessageProperties.basic.copy(
      contentType = Some(ContentType.octetStream),
      contentEncoding = Some(ContentEncoding.utf8),
      headers = Map("foo" -> "fooValue", "bar" -> "barValue"),
      deliveryMode = Some(DeliveryMode.persistent),
      priority = Some(1),
      correlationId = Some(random.nextString(4)),
      replyTo = Some(random.nextString(10)),
      expiration = Some(random.nextString(10)),
      messageId = Some(random.nextString(10)),
      timestamp = Some(new Date()),
      messageType = Some(random.nextString(10)),
      userId = Some(random.nextString(10)),
      appId = Some(random.nextString(10)),
      clusterId = Some(random.nextString(10))
    )

    MessagePropertiesConverters(MessagePropertiesConverters(properties)) shouldBe properties
  }

}
