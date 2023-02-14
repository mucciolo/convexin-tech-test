package com.convexin

import org.apache.spark.rdd.RDD

object IOUtils {

  def parseFileLines(lines: RDD[String]): RDD[(Int, Int)] =
    lines
      .map(splitByCommaOrTab)
      .map(parseInts)
      .filter(notHeader)
      .map(toIntPair)

  private def splitByCommaOrTab(line: String): Array[String] = line.split(Array(',', '\t'))

  private def parseInts(pair: Array[String]): Array[Option[Int]] = pair.map(_.toIntOption)

  private def notHeader(pair: Array[Option[Int]]): Boolean = pair.forall(_.isDefined)

  private def toIntPair(pair: Array[Option[Int]]): (Int, Int) = pair match {
    case Array(Some(key)) => (key, 0)
    case Array(Some(key), Some(value)) => (key, value)
  }

  def pairsToTsvLine(pairs: RDD[(Int, Int)]): RDD[String] = pairs.map {
    case (key, value) => key + "\t" + value
  }

}
