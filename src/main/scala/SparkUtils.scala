package com.convexin

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

  def createSparkContext(maybeAwsProfileName: Option[String]): SparkContext = {
    val conf = createSparkConf(maybeAwsProfileName)
    new SparkContext(conf)
  }

  def createSparkConf(
    maybeAwsProfileName: Option[String],
    awsCredentialsProvider: String => AWSCredentials = new ProfileCredentialsProvider(_)
      .getCredentials
  ): SparkConf = {

    val conf = new SparkConf().setAppName("Convexin Tech Test").setMaster("local[*]")

    maybeAwsProfileName.fold {
      conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    } { profileName =>
      val credentials = awsCredentialsProvider(profileName)
      conf.set("spark.hadoop.fs.s3a.access.key", credentials.getAWSAccessKeyId)
      conf.set("spark.hadoop.fs.s3a.secret.key", credentials.getAWSSecretKey)
    }
  }

  def saveAsTsvFile(columns: RDD[(String, String)], outputPath: String): Unit = {
    columns.map(pairToTsvLine).saveAsTextFile(outputPath)
  }

  def pairToTsvLine(keyValue: (String, String)): String = s"${keyValue._1}\t${keyValue._2}"

}
