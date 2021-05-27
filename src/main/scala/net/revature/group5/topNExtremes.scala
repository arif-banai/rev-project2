package net.revature.group5

import scala.collection.mutable

/**
 *
 * @param storeMax set true to find largest values; set false to find smallest values
 */
class topNExtremes(val storeMax: Boolean, val topN: Int) {
  val topNGrowths: Array[(String, Double)] = new Array[(String, Double)](topN)

  if(storeMax) {
    for(i <- topNGrowths.indices) {
      topNGrowths(i) = ("", 0d)
    }
  } else {
    for(i <- topNGrowths.indices) {
      topNGrowths(i) = ("", Double.MaxValue)
    }
  }


  /**
   * insert, or don't insert, a new growth and state into the array
   *
   * @param newState
   * @param newGrowth
   */
  private def insert(newGrowth: (String, Double)): Unit = {
    var indexToReplace: Int = 0
    for(i <- topNGrowths.indices) {
      if(storeMax) {
        if (topNGrowths(i)._2 < topNGrowths(indexToReplace)._2) indexToReplace = i
        if (newGrowth._2 < topNGrowths(indexToReplace)._2) return
      } else {
        if(topNGrowths(i)._2 > topNGrowths(indexToReplace)._2) indexToReplace = i
        if (newGrowth._2 > topNGrowths(indexToReplace)._2) return
      }
    }

    topNGrowths(indexToReplace) = newGrowth
  }

  /**
   * find the five extremes of the map
   * finds five largest values if storeMax = true
   * finds five smallest values if storeMax = false
   * @param map
   */
  def findExtremes(map: mutable.Map[String, Double]): Unit = {
    map.foreach(growth => {
      this.insert(growth)
    })
  }

  def printExtremes(timePeriod: String): Unit = {
    if(storeMax) println(s"Max growth rate of confirmed cases in the US for $timePeriod:")
    else println(s"Min growth rate of confirmed cases in the US for $timePeriod:")
    for(i <- topNGrowths.indices) {
      println(s"State: ${topNGrowths(i)._1} \t Growth Rate: ${topNGrowths(i)._2}")
    }

    println()
  }

}

object ComplexOrdering extends Ordering[(String, Double)] {
  override def compare(x: (String, Double), y: (String, Double)): Int = {
    x._2.compareTo(y._2)
  }
}