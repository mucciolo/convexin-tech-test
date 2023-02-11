package com.convexin

import SparkUtils._
import UniqueOddCountValuesByKey.uniqueOddCountValuesByKey

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.File
import scala.io.Source
import scala.reflect.io.Directory
import scala.util.Using

class IntegrationTests extends AnyFlatSpec
  with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val ResourcesRoot = "src/it/resources"
  private val OutputPath = s"$ResourcesRoot/output"
  private val SparkConf = new SparkConf().setAppName("IT").setMaster("local[1]")
  private val SparkContext = new SparkContext(SparkConf)

  "uniquePairsByValueOddCount" should "aggregate all directory files" in {
    assertGeneratedFileIsExpected(
      inputPath = s"$ResourcesRoot/multiple-files/*",
      expectedFile =
        """2	4
          |1	-2
          |3	0""".stripMargin
    )
  }

  it should "discard header" in {
    assertGeneratedFileIsExpected(
      inputPath = s"$ResourcesRoot/discard-header.tsv",
      expectedFile = "1\t1"
    )
  }

  it should "default empty values to 0" in {
    assertGeneratedFileIsExpected(
      inputPath = s"$ResourcesRoot/default-value-on-empty.tsv",
      expectedFile = "1\t0"
    )
  }

  it should "drop empty keys" in {
    assertGeneratedFileIsExpected(
      inputPath = s"$ResourcesRoot/drop-empty-keys.csv",
      expectedFile = "1\t1"
    )
  }

  private def assertGeneratedFileIsExpected(inputPath: String, expectedFile: String): Assertion = {
    val actualFile: String = run(inputPath)
    actualFile shouldEqual expectedFile
  }

  private def deleteOutputPath(): Unit = new Directory(new File(OutputPath)).deleteRecursively()

  override protected def beforeAll(): Unit = deleteOutputPath()

  override protected def afterAll(): Unit = SparkContext.stop()

  override protected def afterEach(): Unit = deleteOutputPath()

  private def run(inputPath: String): String = {
    val textLines = SparkContext.textFile(inputPath)
    uniqueOddCountValuesByKey(textLines).coalesce(1).map(pairToTsvLine).saveAsTextFile(OutputPath)
    Using(Source.fromFile(s"$OutputPath/part-00000"))(file => file.getLines().mkString("\n")).get
  }
}