package com.mucciolo


import CoreLogic._

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should._

final class CoreLogicSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  private val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[1]")
  private val sc = new SparkContext(conf)

  override protected def afterAll(): Unit = sc.stop()

  "uniqueOddCountValuesByKey" - {

    "should drop a pair with an even number of occurrences" in {
      val pairs = sc.parallelize(Seq((1, 2), (1, 2)))

      oddOccurrencesByKey(pairs).collect() shouldBe Array.empty
    }

    "should return a single instance of a pair with an odd number of occurrences" in {
      val pairs = sc.parallelize(Seq((1, 1), (1, 1), (1, 1)))

      oddOccurrencesByKey(pairs).collect() shouldBe Array((1, 1))
    }

    "should drop even count pairs and return a single instance of odd count pairs" in {
      val pairs = sc.parallelize(Seq(
        (1, 2), (2, 3), (1, 1), (2, 3), (1, 2), (2, 3), (2, 2), (2, 2)
      ))

      oddOccurrencesByKey(pairs).collect() shouldBe Array((1, 1), (2, 3))
    }
  }

}
