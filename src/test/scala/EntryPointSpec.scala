package com.mucciolo

import EntryPoint.parseArguments

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should._

final class EntryPointSpec extends AnyFreeSpec with Matchers {

  "parseArguments" - {
    "should throw IllegalArgumentException given 0 arguments" in {
      val args = Array.empty[String]
      an [IllegalArgumentException] should be thrownBy parseArguments(args)
    }

    "should throw IllegalArgumentException given 1 argument" in {
      val args = Array("input/path")
      an[IllegalArgumentException] should be thrownBy parseArguments(args)
    }

    "should return a triple without a profile name given 2 arguments" in {
      val args = Array("input/path", "output/path")
      parseArguments(args) should be ("input/path", "output/path", None)
    }

    "should return a triple with AWS profile name given 3 arguments" in {
      val args = Array("input/path", "output/path", "aws-profile")
      parseArguments(args) should be("input/path", "output/path", Some("aws-profile"))
    }
  }
}
