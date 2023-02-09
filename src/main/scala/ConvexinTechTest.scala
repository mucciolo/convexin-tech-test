package com.convexin

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.pi.model.InvalidArgumentException
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ConvexinTechTest {

  private val Separators = Array(',', '\t')
  private val DefaultValue = "0"

  def main(args: Array[String]): Unit = {

    // TODO use Either
    val (inputPath, outputPath, awsProfileName) = args match {
      case Array() =>
        throw new InvalidArgumentException("Missing both input and output paths")

      case Array(_) =>
        throw new InvalidArgumentException("Missing output path")

      case Array(inputPath, outputPath) =>
        (inputPath, outputPath, None)

      case Array(inputPath, outputPath, awsProfileName, _*) =>
        (inputPath, outputPath, Some(awsProfileName))
    }

    val credentialsProvider = awsProfileName.map(new ProfileCredentialsProvider(_))
      .getOrElse(new DefaultAWSCredentialsProviderChain)
    val credentials = credentialsProvider.getCredentials

    implicit val sc: SparkContext = createSparkContext(credentials)
    uniquePairsByValueOddCount(inputPath).saveAsTextFile(outputPath)

  }

  def uniquePairsByValueOddCount(inputPath: String)(implicit sc: SparkContext): RDD[String] = {

    val inputFiles = sc.textFile(inputPath)
    val keyValuePairs: RDD[(String, String)] =
      inputFiles.filter(isValidLine).map(splitLine).collect(nonEmptyArraysAsPair)
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
    case Array(key) if key.nonEmpty => key -> DefaultValue
    case Array(key, value, _*) if key.nonEmpty => key -> value
  }

  def splitLine(line: String): Array[String] = {
    line.split(Separators)
  }

  def isValidLine(line: String): Boolean = {
    line.matches("""^-?\d+[,\t](?:-?\d+$)?""")
  }

  def createSparkContext(credentials: AWSCredentials, threadsNum: String = "*"): SparkContext = {
    val conf = new SparkConf()
      .setAppName("Convexin Tech Test")
      .setMaster(s"local[$threadsNum]")
      .set("spark.hadoop.fs.s3a.access.key", credentials.getAWSAccessKeyId)
      .set("spark.hadoop.fs.s3a.secret.key", credentials.getAWSSecretKey)
    //      .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    //      .set("spark.hadoop.fs.s3a.path.style.access", "true")

    new SparkContext(conf)
  }

}
