package com.convexin

import SparkUtils._

import com.amazonaws.auth.BasicAWSCredentials
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should._

final class SparkUtilsSpec extends AnyFreeSpec with Matchers {

  private val AwsCredentialsProvider =
    (_: String) => new BasicAWSCredentials("access", "secret")

  "createSparkConf" - {
    "should setup credentials given an existing profile name" in {
      val profileName = Some("test-profile")
      val conf = createSparkConf(profileName, AwsCredentialsProvider)

      conf.get("spark.hadoop.fs.s3a.access.key") shouldBe "access"
      conf.get("spark.hadoop.fs.s3a.secret.key") shouldBe "secret"
    }

    "should default to DefaultAWSCredentialsProviderChain given no profile name" in {
      val profileName = None
      val conf = createSparkConf(profileName, AwsCredentialsProvider)

      conf.contains("spark.hadoop.fs.s3a.access.key") shouldBe false
      conf.contains("spark.hadoop.fs.s3a.secret.key") shouldBe false
      conf.get("spark.hadoop.fs.s3a.aws.credentials.provider") shouldBe
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }
  }

  "toTsvLine" - {
    "should convert a pair to tab-separated string" in {
      toTsvLine(("1", "2")) should be ("1\t2")
    }
  }

}
