package itv.bucky

import org.scalatest.FunSuite

import scala.util.{Failure, Try, Success}
import org.scalatest.Matchers._

class TryUtilTest extends FunSuite {

  test("List of successes should give successes") {
    val list = List[Try[Int]](Try(1), Try(2), Try(3))
    val result: Try[List[Int]] = TryUtil.sequence(list)

    result shouldBe Try(List(1, 2, 3))
  }

  test("List containing a failure should give a failure") {
    val exception = new RuntimeException("What number comes next?")
    val list = List[Try[Int]](Try(1), Try { throw exception }, Try(3))
    val result: Try[List[Int]] = TryUtil.sequence(list)

    result match {
      case Success(_) => fail("Expected a failure")
      case Failure(e) => e shouldBe exception
    }
  }

}
