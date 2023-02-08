ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.10"

val SparkVersion = "3.3.0"
val HadoopVersion = "3.3.4"
val Slf4jVersion = "2.0.6"
val AwsSdkVersion = "1.12.402"
val ScalaTestVersion = "3.2.15"

lazy val root = (project in file("."))
  .settings(
    name := "convexin-tech-test",
    idePackagePrefix := Some("com.convexin"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % SparkVersion,
      "org.apache.hadoop" % "hadoop-aws" % HadoopVersion,
      "com.amazonaws" % "aws-java-sdk" % AwsSdkVersion,
      "org.scalatest" %% "scalatest" % ScalaTestVersion % "test"
    )
  )
