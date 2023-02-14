package com.mucciolo

import org.apache.spark.rdd.RDD

object CoreLogic {

  def oddOccurrencesByKey(pairs: RDD[(Int, Int)]): RDD[(Int, Int)] =
    pairs
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter(hasOddCount)
      .keys

  private def hasOddCount(pairCount: (_, Int)): Boolean = pairCount match {
    case (_, count) => count % 2 == 1
  }

}
