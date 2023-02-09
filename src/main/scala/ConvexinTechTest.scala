package com.convexin

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

sealed trait ArgumentsParseError
case class MissingInput(inputs: String*) extends ArgumentsParseError

case class ParsedArgs(inputPath: String, outputPath: String, awsProfileName: Option[String])

object ConvexinTechTest {

  private val Separators = Array(',', '\t')
  private val DefaultValue = "0"

  def main(args: Array[String]): Unit = {
    parseArguments(args).fold(printError, runJob)
  }

  private def printError(error: ArgumentsParseError): Unit = error match {
    case MissingInput(inputs @ _*) =>
      println(
        inputs.mkString("Missing arguments: ",
          ", ",
          "\nsbt run <input-path> <output-path> ?<aws-profile>")
      )
  }

  private def runJob(parsedArgs: ParsedArgs): Unit = {
    val credentials = getAWSCredentials(parsedArgs.awsProfileName)
    implicit val sc: SparkContext = createSparkContext(credentials)
    uniquePairsByValueOddCount(parsedArgs.inputPath).saveAsTextFile(parsedArgs.outputPath)
    sc.stop()
  }

  def parseArguments(args: Array[String]): Either[ArgumentsParseError, ParsedArgs] = {

    args match {
      case Array() =>
        Left(MissingInput("input path", "output path"))

      case Array(_) =>
        Left(MissingInput("output path"))

      case Array(inputPath, outputPath) =>
        Right(ParsedArgs(inputPath, outputPath, None))

      case Array(inputPath, outputPath, awsProfileName, _*) =>
        Right(ParsedArgs(inputPath, outputPath, Some(awsProfileName)))
    }

  }

  private def getAWSCredentials(awsProfileName: Option[String]) = {
    awsProfileName
      .map(new ProfileCredentialsProvider(_))
      .getOrElse(new DefaultAWSCredentialsProviderChain)
      .getCredentials
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

  def createSparkContext(credentials: AWSCredentials, threadsNum: Option[Int] = None): SparkContext = {
    val conf = new SparkConf()
      .setAppName("Convexin Tech Test")
      .setMaster(s"local[${threadsNum.getOrElse("*")}]")
      .set("spark.hadoop.fs.s3a.access.key", credentials.getAWSAccessKeyId)
      .set("spark.hadoop.fs.s3a.secret.key", credentials.getAWSSecretKey)
    //      .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    //      .set("spark.hadoop.fs.s3a.path.style.access", "true")

    new SparkContext(conf)
  }

}
