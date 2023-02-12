package com.convexin


import UniqueOddCountValuesByKey._

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should._

final class UniqueOddCountValuesByKeySpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  private val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[1]")
  private val sc = new SparkContext(conf)

  override protected def afterAll(): Unit = sc.stop()

  "splitByCommaOrTab" - {
    "should split comma-separated values" in {
      splitByCommaOrTab("1,2") should be (Array("1", "2"))
    }

    "should split tab-separated values" in {
      splitByCommaOrTab("1\t2") should be (Array("1", "2"))
    }
  }

  "areAllElementsIntegers" - {
    "should return true given an array of integers" in {
      val integers = (-9 to 9).map(_.toString).toArray
      areAllElementsIntegers(integers) should be (true)
    }

    "should return false given an array containing a non-integer" in {
      val numbers = ('0' to '9').toSet
      val nonIntegers = (32 to 126).map(_.toChar).filterNot(numbers).map(_.toString)

      Inspectors.forAll(nonIntegers) { c =>
        areAllElementsIntegers(Array("1", c)) should be (false)
      }
    }
  }

  "toKeyValuePair" - {
    "should convert an array of two elements representing the key and value columns to a pair" in {
      val pairAsArray = Array("1", "2")
      toKeyValuePair(pairAsArray) should be ("1", "2")
    }

    "should default second element to 0 when value is missing" in {
      val pairAsArray = Array("1")
      toKeyValuePair(pairAsArray) should be ("1", "0")
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
      uniqueOddCountValuesByKey(lines).collect() should be (Array(("2", "3"), ("1", "1")))
    }
  }

}
