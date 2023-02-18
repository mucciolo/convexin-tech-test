package com.mucciolo

import SparkUtils.SparkConfKeys._

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.hadoop.fs.s3a.Constants._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

  object SparkConfKeys {
    private val HadoopConfKeyRoot: String = "spark.hadoop."
    val AwsCredentialsProvider: String = HadoopConfKeyRoot + AWS_CREDENTIALS_PROVIDER
    val AwsAccessKey: String = HadoopConfKeyRoot + ACCESS_KEY
    val AwsSecretKey: String = HadoopConfKeyRoot + SECRET_KEY
  }

  def createSparkContext(maybeAwsProfileName: Option[String]): SparkContext = {
    val conf = createSparkConf(maybeAwsProfileName)
    new SparkContext(conf)
  }

  def createSparkConf(
    maybeAwsProfileName: Option[String],
    awsCredentialsProvider: String => AWSCredentials = {
      new ProfileCredentialsProvider(_).getCredentials
    }
  ): SparkConf = {

    val conf = new SparkConf().setAppName("Spark S3 Integration").setMaster("local[*]")

    maybeAwsProfileName match {
      case None =>
        conf.set(AwsCredentialsProvider, "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      case Some(awsProfileName) =>
        val credentials = awsCredentialsProvider(awsProfileName)
        conf.set(AwsAccessKey, credentials.getAWSAccessKeyId)
        conf.set(AwsSecretKey, credentials.getAWSSecretKey)
    }
  }

}
