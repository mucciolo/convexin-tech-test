package com.mucciolo

import EntryPoint.main

import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should._

import scala.reflect.io.{Directory, Path}

final class FileSystemIntegrationTest extends AnyFreeSpec with Matchers with BeforeAndAfterEach {

  private val ResourcesRoot = "src/it/resources"
  private val InputPath = s"$ResourcesRoot/input/*"
  private val OutputPath = s"$ResourcesRoot/output"

  "the application" - {
    "should consume all files in the input directory and output the result as TSV" in {

      main(args = Array(InputPath, OutputPath, "test-profile"))

      val partitionsOutput = Directory(OutputPath).list.filter(_.name.startsWith("part-"))
      val resultLines = partitionsOutput.flatMap(_.toFile.lines()).toSet
      val expectedLines = Set("2\t4", "1\t-2", "3\t0")

      resultLines shouldEqual expectedLines
    }
  }

  override protected def afterEach(): Unit = Path(OutputPath).deleteRecursively()

}
