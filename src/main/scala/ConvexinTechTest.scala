package com.convexin

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ConvexinTechTest {

  def main(args: Array[String]): Unit = {
    parseArguments(args) match {

      case Left(error) =>
        println(error)
        System.exit(1)

      case Right((inputPath, outputPath, maybeAwsProfileName)) =>
        runSparkJob(inputPath, outputPath, maybeAwsProfileName)

    }
  }

  private def parseArguments(
    args: Array[String]
  ): Either[String, (String, String, Option[String])] =
    args match {
      case Array(inputPath, outputPath) =>
        Right((inputPath, outputPath, None))

      case Array(inputPath, outputPath, awsProfileName, _*) =>
        Right((inputPath, outputPath, Some(awsProfileName)))

      case Array() | Array(_) =>
        Left("Missing arguments. Usage: sbt run <input-path> <output-path> ?<aws-profile>")
    }

  private def runSparkJob(inputPath: String,
                          outputPath: String,
                          maybeAwsProfileName: Option[String]): Unit = {

    implicit val sc: SparkContext = createSparkContext(maybeAwsProfileName)

    uniquePairsByValueOddCount(inputPath).saveAsTextFile(outputPath)
    sc.stop()

  }

  def uniquePairsByValueOddCount(inputPath: String)(implicit sc: SparkContext): RDD[String] = {

    val inputFiles = sc.textFile(inputPath)
    val keyValuePairs = inputFiles.filter(isValidLine).map(splitLine).collect(nonEmptyArraysAsPair)
    val counts = keyValuePairs.map((_, 1)).reduceByKey(_ + _)
    val oddCounts = counts.collect(oddCountPairs)
    val result = oddCounts.map(pairToTsvLine)

    result.coalesce(1)
  }

  def pairToTsvLine(pair: (String, String)): String = pair match {
    case (key, value) => s"$key\t$value"
  }

  val oddCountPairs: PartialFunction[((String, String), Int), (String, String)] = {
    case (keyValuePair, count) if count % 2 != 0 => keyValuePair
  }

  val nonEmptyArraysAsPair: PartialFunction[Array[String], (String, String)] = {
    case Array(key) if key.nonEmpty => key -> "0"
    case Array(key, value, _*) if key.nonEmpty => key -> value
  }

  def splitLine(line: String): Array[String] = {
    line.split(Array(',', '\t'))
  }

  def isValidLine(line: String): Boolean = {
    line.matches("""^-?\d+[,\t](?:-?\d+$)?""")
  }

  def createSparkContext(maybeAwsProfileName: Option[String]): SparkContext = {

    val credentialsConf = maybeAwsProfileName match {
      case Some(profileName) =>
        val credentials = new ProfileCredentialsProvider(profileName).getCredentials
          List(
            "spark.hadoop.fs.s3a.access.key" -> credentials.getAWSAccessKeyId,
            "spark.hadoop.fs.s3a.secret.key" -> credentials.getAWSSecretKey
          )
      case None =>
        List(
          "spark.hadoop.fs.s3a.aws.credentials.provider" ->
          "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
    }

    val conf = new SparkConf()
      .setAppName("Convexin Tech Test")
      .setMaster("local[*]")
      .setAll(credentialsConf)

    new SparkContext(conf)
  }

}
