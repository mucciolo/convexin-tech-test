package com.convexin

import Spark._
import UniqueOddCountValuesByKey.uniqueOddCountValuesByKey

object EntryPoint {

  def main(args: Array[String]): Unit = {
    val (inputPath, outputPath, maybeAwsProfileName) = parseArguments(args)
    run(inputPath, outputPath, maybeAwsProfileName)
  }

  def parseArguments(args: Array[String]): (String, String, Option[String]) =
    args match {
      case Array(inputPath, outputPath) =>
        (inputPath, outputPath, None)

      case Array(inputPath, outputPath, awsProfileName, _*) =>
        (inputPath, outputPath, Some(awsProfileName))

      case _ =>
        throw new IllegalArgumentException(
          "Missing arguments. Usage: sbt run <input-path> <output-path> ?<aws-profile>")
    }

  def run(inputPath: String, outputPath: String, maybeAwsProfileName: Option[String]): Unit = {

    val sc = createSparkContext(maybeAwsProfileName)
    val filesLines = sc.textFile(inputPath)
    val oddCountPairs = uniqueOddCountValuesByKey(filesLines)

    saveAsTsvFile(oddCountPairs, outputPath)
    sc.stop()
  }
}
