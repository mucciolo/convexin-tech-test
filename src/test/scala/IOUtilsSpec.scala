package com.mucciolo

import IOUtils._

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class IOUtilsSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  private val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[1]")
  private val sc = new SparkContext(conf)

  override protected def afterAll(): Unit = sc.stop()

  "parseFileLines" - {
    "should handle empty files" in {
      val lines = sc.emptyRDD[String]
      val actualOutput = parseFileLines(lines).collect()
      val expectedOutput = Array.empty

      actualOutput shouldBe expectedOutput
    }

    "should handle comma-separated values files" in {
      val lines = sc.parallelize(Seq("1,1", "2,-2", "-3,3", "-4,-4"))
      val actualOutput = parseFileLines(lines).collect()
      val expectedOutput = Array((1, 1), (2, -2), (-3, 3), (-4, -4))

      actualOutput shouldBe expectedOutput
    }

    "should handle tab-separated values files" in {
      val lines = sc.parallelize(Seq("1\t1", "2\t-2", "-3\t3", "-4\t-4"))
      val actualOutput = parseFileLines(lines).collect()
      val expectedOutput = Array((1, 1), (2, -2), (-3, 3), (-4, -4))

      actualOutput shouldBe expectedOutput
    }

    "should handle concatenated comma-separated and tab-separated values files" in {
      val lines = sc.parallelize(Seq("1,1", "2\t-2", "-3,3", "-4\t-4"))
      val actualOutput = parseFileLines(lines).collect()
      val expectedOutput = Array((1, 1), (2, -2), (-3, 3), (-4, -4))

      actualOutput shouldBe expectedOutput
    }

    "should drop file headers" in {
      val lines = sc.parallelize(Seq("key,val", "1,1", "2,2"))
      val actualOutput = parseFileLines(lines).collect()
      val expectedOutput = Array((1, 1), (2, 2))

      actualOutput shouldBe expectedOutput
    }

    "should drop concatenated files headers" in {
      val lines = sc.parallelize(Seq("key,val", "1,1", "2,2", "foo#\t*bar", "3\t3"))
      val actualOutput = parseFileLines(lines).collect()
      val expectedOutput = Array((1, 1), (2, 2), (3, 3))

      actualOutput shouldBe expectedOutput
    }

    "should default empty values to 0" in {
      val lines = sc.parallelize(Seq("1,", "1,1", "2\t", "2\t2"))
      val actualOutput = parseFileLines(lines).collect()
      val expectedOutput = Array((1, 0), (1, 1), (2, 0), (2, 2))

      actualOutput shouldBe expectedOutput
    }
  }

  "pairsToTsvLine" - {
    "should convert integer pairs to a tab-separated values string" in {
      val pairs = sc.parallelize(Seq((1, 1), (1, -2)))
      val actualOutput = pairsToTsvLine(pairs).collect()
      val expectedOutput = Array("1\t1", "1\t-2")

      actualOutput shouldBe expectedOutput
    }
  }

}
