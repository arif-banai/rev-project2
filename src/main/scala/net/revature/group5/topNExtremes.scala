package net.revature.group5

import scala.collection.mutable

/**
 * @param storeMax set true to find largest values; set false to find smallest values
 * @param topN how many extremes you want to find
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
   * @param newGrowth
   */
  private def insert(newGrowth: (String, Double)): Unit = {
    var indexToReplace: Int = 0
    val indices = Array.range(0, topNGrowths.length)
    if(storeMax) {
      for(i <- indices) {
        if (topNGrowths(i)._2 < topNGrowths(indexToReplace)._2) indexToReplace = i
      }
      if (newGrowth._2 < topNGrowths(indexToReplace)._2) return
    } else {
      for(i <- indices) {
        if (topNGrowths(i)._2 > topNGrowths(indexToReplace)._2) indexToReplace = i
      }
      if (newGrowth._2 > topNGrowths(indexToReplace)._2) return
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

  /**
   * prints the states and growth rates for a certain timePeriod
   * @param timePeriod
   */
  def printExtremes(timePeriod: String): Unit = {
    if(storeMax) println(s"Max growth rate of confirmed cases in the US for $timePeriod:")
    else println(s"Min growth rate of confirmed cases in the US for $timePeriod:")
    val topNGrowthsSorted = topNGrowths.sortWith(_._2 > _._2)
    for(i <- topNGrowths.indices) {
      println(f"State: ${topNGrowthsSorted(i)._1}%-28s Growth Rate: ${topNGrowthsSorted(i)._2}%9.2f%%")
    }

    println()
  }

}

object ComplexOrdering extends Ordering[(String, Double)] {
  override def compare(x: (String, Double), y: (String, Double)): Int = {
    x._2.compareTo(y._2)
  }
}