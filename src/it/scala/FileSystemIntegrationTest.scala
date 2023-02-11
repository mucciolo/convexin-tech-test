package com.convexin

import EntryPoint.main

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

import scala.reflect.io.{Directory, Path}

final class FileSystemIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val ResourcesRoot = "src/it/resources"
  private val InputPath = s"$ResourcesRoot/input/*"
  private val OutputPath = s"$ResourcesRoot/output"

  "The program" should
    "process all files in the input directory and save the result to the output directory" in {

    main(args = Array(InputPath, OutputPath, "test"))

    val partitionsOutput = Directory(OutputPath).list.filter(_.name.startsWith("part-"))
    val resultLines = partitionsOutput.flatMap(_.toFile.lines()).toSet
    val expectedLines = Set("2\t4", "1\t-2", "3\t0")

    resultLines shouldEqual expectedLines
  }

  override protected def beforeEach(): Unit = Path(OutputPath).deleteRecursively()

}
