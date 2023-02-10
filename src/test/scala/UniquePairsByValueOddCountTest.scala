package com.convexin

import ConvexinTechTest._

import org.scalatest.matchers.should._
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.wordspec.AnyWordSpec

class UniquePairsByValueOddCountTest extends AnyWordSpec with Matchers {

  private val ValidLines = Table(
    "line",
    "0,",
    "1,1",
    "2\t2",
    "-3\t3",
    "4\t-4",
    "-5\t-5",
  )

  "A raw valid line" should {
    "consist of one integer followed by or two integers separated by a comma or tab" in {
      forAll(ValidLines)(isValidLine)
    }

    "not contain alpha or special characters" in {

      val numbers = (' ' to '9').toSet
      val invalidChars = ('!' to '~').filterNot(numbers.contains)

      assert(
        invalidChars.flatMap(c => List(s"1,$c", s"$c,1", s"$c,$c", s"1\t$c", s"$c\t1", s"$c\t$c"))
          .forall(!isValidLine(_))
      )
    }

    "be split in 1 or 2 elements Array" in {
      forAll(ValidLines) { validLine =>
        splitLine(validLine) match {
          case Array(_) | Array(_, _) => true
        }
      }
    }
  }

  "A valid line array" should {
    "default value to 0 on pair conversion" in {
      nonEmptyArrayToPair(Array("1")) shouldBe ("1", "0")
    }

    "be converted to a pair" in {
      nonEmptyArrayToPair(Array("2", "2")) shouldBe ("2", "2")
    }
  }

  "A pair count" should {
    "apply when odd" in {
      assert(hasOddCount(("1", "1"), 3))
    }

    "be filtered when even" in {
      assert(!hasOddCount(("2", "2"), 4))

    }
  }

  "A pair" should {
    "be converted to a TSV line" in {
      pairToTsvLine(("1", "2")) shouldBe "1\t2"
    }
  }

}
