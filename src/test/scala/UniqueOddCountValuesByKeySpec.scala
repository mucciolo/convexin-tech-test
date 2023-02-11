package com.convexin


import UniqueOddCountValuesByKey._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should._
import org.scalatest._

import scala.reflect.ClassTag

final class UniqueOddCountValuesByKeySpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  private val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[1]")
  private val sc = new SparkContext(conf)

  override protected def afterAll(): Unit = sc.stop()

  private def assertRddIsExpected[T: ClassTag](actual: RDD[T], expected: T*): Assertion =
    actual.collect() should be (expected.toArray)

  "splitLinesIntoArray" - {
    "should split comma-separated values" in {
      val lines = sc.parallelize(Seq("1,2", "3,4"))
      assertRddIsExpected(
        actual = splitLinesIntoArray(lines),
        expected = Array("1", "2"), Array("3", "4")
      )
    }

    "should split tab-separated values" in {
      val lines = sc.parallelize(Seq("1\t2", "3\t4"))
      assertRddIsExpected(
        actual = splitLinesIntoArray(lines),
        expected = Array("1", "2"), Array("3", "4")
      )
    }
  }

  "areIntegers" - {
    "should return true given an array of integers" in {
      val integers = (-9 to 9).map(_.toString).toArray
      areIntegers(integers) should be (true)
    }

    "should return false given an array containing a non-integer" in {
      val numbers = ('0' to '9').toSet
      val nonIntegers = (32 to 126).map(_.toChar).filterNot(numbers).map(_.toString)

      Inspectors.forAll(nonIntegers) { c =>
        areIntegers(Array("1", c)) should be (false)
      }
    }
  }

  "nonEmptyArrayToPair" - {
    "should convert an array of two elements to a pair" in {
      val pairAsArray = Array("1", "2")
      nonEmptyArrayToPair(pairAsArray) should be ("1", "2")
    }

    "should default second element (value) to 0" in {
      val pairAsArray = Array("1")
      nonEmptyArrayToPair(pairAsArray) should be ("1", "0")
    }
  }

  "countPairOccurrences" - {
    "should count the number of occurrences of a pair" in {
      val pairs = sc.parallelize(Seq(("1", "1"), ("1", "2"), ("3", "5"), ("1", "2")))
      assertRddIsExpected(
        actual = countPairOccurrences(pairs),
        expected = (("3", "5"), 1), (("1", "1"), 1), (("1", "2"), 2)
      )
    }
  }

  "hasOddCount" - {
    "should return true given an odd count of pairs" in {
      val keyValueCount = (("3", "5"), 1)
      hasOddCount(keyValueCount) should be (true)
    }

    "should return false given an even count of pairs" in {
      val keyValueCount = (("1", "1"), 2)
      hasOddCount(keyValueCount) should be (false)
    }
  }

  "uniqueOddCountValuesByKey" - {
    "should return the pairs which the value occurs an odd number of times for the key" in {
      val lines = sc.parallelize(Seq("1,2", "2,3", "1,1", "2,3", "1,2", "2,3", "2,2", "2,2"))
      assertRddIsExpected(
        actual = uniqueOddCountValuesByKey(lines),
        expected = ("2", "3"), ("1", "1")
      )
    }
  }

}
