package com.mucciolo

import org.apache.spark.rdd.RDD

object UniqueOddCountValuesByKey {

  def uniqueOddCountValuesByKey(lines: RDD[String]): RDD[(String, String)] =
    lines
      .map(splitByCommaOrTab)
      .filter(areAllElementsIntegers)
      .map(toKeyValuePair)
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter(hasOddCount)
      .keys

  def splitByCommaOrTab(line: String): Array[String] = line.split(Array(',', '\t'))

  def areAllElementsIntegers(data: Array[String]): Boolean = data.forall(_.matches("-?[0-9]+"))

  def toKeyValuePair(arr: Array[String]): (String, String) =
    (arr(0), if (arr.length >= 2) arr(1) else "0")

  def hasOddCount(keyValueCount: ((String, String), Int)): Boolean = keyValueCount._2 % 2 == 1

}
