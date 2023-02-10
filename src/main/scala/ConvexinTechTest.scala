package com.convexin

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ConvexinTechTest {

  private val Separators = Array(',', '\t')

  def main(args: Array[String]): Unit = {
    val (inputPath, outputPath, maybeAwsProfileName) = parseArguments(args)
    runSparkJob(inputPath, outputPath, maybeAwsProfileName)
  }

  private def parseArguments(args: Array[String]): (String, String, Option[String]) =
    args match {
      case Array(inputPath, outputPath) =>
        (inputPath, outputPath, None)

      case Array(inputPath, outputPath, awsProfileName, _*) =>
        (inputPath, outputPath, Some(awsProfileName))

      case _ =>
        throw new IllegalArgumentException(
          "Missing arguments. Usage: sbt run <input-path> <output-path> ?<aws-profile>")
    }

  private def runSparkJob(inputPath: String,
                          outputPath: String,
                          maybeAwsProfileName: Option[String]): Unit = {
    val sc  = createSparkContext(maybeAwsProfileName)
    uniquePairsByValueOddCount(sc, inputPath).saveAsTextFile(outputPath)
    sc.stop()
  }

  private def createSparkContext(maybeAwsProfileName: Option[String]): SparkContext = {
    val conf = createSparkConf(maybeAwsProfileName)
    new SparkContext(conf)
  }

  private def createSparkConf(maybeAwsProfileName: Option[String]) = {

    val conf = new SparkConf().setAppName("Convexin Tech Test").setMaster("local[*]")

    maybeAwsProfileName match {
      case Some(profileName) =>
        val credentials = new ProfileCredentialsProvider(profileName).getCredentials
        conf.set("spark.hadoop.fs.s3a.access.key", credentials.getAWSAccessKeyId)
        conf.set("spark.hadoop.fs.s3a.secret.key", credentials.getAWSSecretKey)
      case None =>
        conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
          "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    }

    conf
  }

  def uniquePairsByValueOddCount(sc: SparkContext, inputPath: String): RDD[String] = {
    sc.textFile(inputPath)
      .filter(isValidLine)
      .map(splitLine)
      .filter(_.head.nonEmpty)
      .map(nonEmptyArrayToPair)
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter(hasOddCount)
      .map(_._1)
      .map(pairToTsvLine)
      .coalesce(1)
  }

  def pairToTsvLine(pair: (String, String)): String = s"${pair._1}\t${pair._2}"

  def hasOddCount(keyValueCount: (_, Int)): Boolean = keyValueCount._2 % 2 != 0

  def nonEmptyArrayToPair(arr: Array[String]): (String, String) = arr match {
    case Array(key) => key -> "0"
    case Array(key, value) => key -> value
  }

  def splitLine(line: String): Array[String] = line.split(Separators)

  def isValidLine(line: String): Boolean =
    line.forall(c => c.isDigit || Separators.contains(c) || c == '-')

}
