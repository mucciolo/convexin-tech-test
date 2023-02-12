package com.convexin

import SparkUtils._
import UniqueOddCountValuesByKey.uniqueOddCountValuesByKey

object EntryPoint {

  def main(args: Array[String]): Unit = {
    val (inputPath, outputPath, maybeAwsProfileName) = parseArguments(args)
    run(inputPath, outputPath, maybeAwsProfileName)
  }

  def parseArguments(args: Array[String]): (String, String, Option[String]) = {
    val argsOpt = args.lift

    (argsOpt(0), argsOpt(1), argsOpt(2)) match {
      case (Some(inputPath), Some(outputPath), maybeAwsProfileName) =>
        (inputPath, outputPath, maybeAwsProfileName)

      case _ =>
        throw new IllegalArgumentException(
          "Missing arguments. Usage: sbt run input-path output-path [aws-profile-name]")
    }
  }

  private def run(
    inputPath: String, outputPath: String, maybeAwsProfileName: Option[String]
  ): Unit = {

    val sc = createSparkContext(maybeAwsProfileName)

    try {
      val filesLines = sc.textFile(inputPath)
      val oddCountPairs = uniqueOddCountValuesByKey(filesLines)

      saveAsTsvFile(oddCountPairs, outputPath)
    } finally {
      sc.stop()
    }

  }

}
