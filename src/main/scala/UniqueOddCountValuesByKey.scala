package com.convexin

import org.apache.spark.rdd.RDD

object UniqueOddCountValuesByKey {

  def uniqueOddCountValuesByKey(lines: RDD[String]): RDD[(String, String)] = {
    val integerPairs = splitLinesIntoArray(lines).filter(areIntegers).map(nonEmptyArrayToPair)
    countPairOccurrences(integerPairs).filter(hasOddCount).map(_._1)
  }

  def splitLinesIntoArray(fileLines: RDD[String]): RDD[Array[String]] = {
    fileLines.map(_.split(Array(',', '\t')))
  }

  def areIntegers(data: Array[String]): Boolean = data.forall(_.matches("-?[0-9]+"))

  def nonEmptyArrayToPair(arr: Array[String]): (String, String) =
    (arr(0), if (arr.length >= 2) arr(1) else "0")

  def countPairOccurrences(integerPairs: RDD[(String, String)]): RDD[((String, String), Int)] = {
    integerPairs.map((_, 1)).reduceByKey(_ + _)
  }

  def hasOddCount(keyValueCount: (_, Int)): Boolean = keyValueCount._2 % 2 != 0

}
