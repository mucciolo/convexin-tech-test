package com.convexin

import com.convexin.ConvexinTechTest.parseArguments
import org.scalatest.EitherValues
import org.scalatest.matchers.should._
import org.scalatest.wordspec.AnyWordSpec

class ArgumentsParseTest extends AnyWordSpec with Matchers with EitherValues {

  "Arguments parse" should {
    "fail given no arguments" in {
      val actualError = parseArguments(Array.empty).left.value
      val expectedError = MissingInput("input path", "output path")

      actualError shouldBe expectedError
    }

    "fail given a single argument" in {
      val actualError = parseArguments(Array("input/path")).left.value
      val expectedError = MissingInput("output path")

      actualError shouldBe expectedError
    }

    "return parsed arguments given two arguments" in {
      val actualParsedArgs = parseArguments(Array("input/path", "output/path")).value
      val expectedParsedArgs = ParsedArgs("input/path", "output/path", None)

      actualParsedArgs shouldBe expectedParsedArgs
    }

    "return parsed arguments given three arguments" in {
      val actualParsedArgs = parseArguments(Array("input/path", "output/path", "profile")).value
      val expectedParsedArgs = ParsedArgs("input/path", "output/path", Some("profile"))

      actualParsedArgs shouldBe expectedParsedArgs
    }
  }

}
