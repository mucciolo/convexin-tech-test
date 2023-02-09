package com.convexin

import ConvexinTechTest._

import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.SparkContext
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

import java.io.File
import scala.io.Source
import scala.reflect.io.Directory
import scala.util.Using

class UniqueKeyValuePairOddCountsTest extends AnyFlatSpec
  with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val OutputPath = "src/test/resources/output"
  private val TestCredentials = new BasicAWSCredentials("test-key", "test-secret")
  private implicit val SparkContext: SparkContext = createSparkContext(TestCredentials, threadsNum = "1")

  "uniquePairsByValueOddCount" should "aggregate all directory files" in {
    assertGeneratedFileIsExpected(
      inputPath = "src/test/resources/multiple-files/*",
      expectedFile =
        """2	4
          |3	0
          |1	2""".stripMargin
    )
  }

  it should "discard header" in {
    assertGeneratedFileIsExpected(
      inputPath = "src/test/resources/discard-header.csv",
      expectedFile = "1\t1"
    )
  }

  it should "default empty values to 0" in {
    assertGeneratedFileIsExpected(
      inputPath = "src/test/resources/default-value-on-empty.tsv",
      expectedFile = "1\t0"
    )
  }

  it should "drop empty keys" in {
    assertGeneratedFileIsExpected(
      inputPath = "src/test/resources/drop-empty-keys.csv",
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
    uniquePairsByValueOddCount(inputPath).saveAsTextFile(OutputPath)
    Using(Source.fromFile(s"$OutputPath/part-00000"))(file => file.getLines().mkString("\n")).get
  }
}
