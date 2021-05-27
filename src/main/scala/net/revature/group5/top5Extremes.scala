package net.revature.group5

import scala.collection.mutable

/**
 *
 * @param storeMax set true to find largest values; set false to find smallest values
 */
class top5Extremes(val storeMax: Boolean) {
  val top5Growths: Array[Double] = new Array[Double](5)
  val top5States:  Array[String] = new Array[String](5)

  if(storeMax) {
    for(i <- top5Growths.indices) {
      top5Growths(i) = 0.0
    }
  } else {
    for(i <- top5Growths.indices) {
      top5Growths(i) = Double.MaxValue
    }
  }


  /**
   * insert, or don't insert, a new growth and state into the array
   *
   * @param newState
   * @param newGrowth
   */
  private def insert(newState: String, newGrowth: Double): Unit = {
    var indexToReplace: Int = 0
    for(i <- top5Growths.indices) {
      if(storeMax) {
        if (top5Growths(i) < top5Growths(indexToReplace)) indexToReplace = i
        if (newGrowth < top5Growths(indexToReplace)) return
      } else {
        if(top5Growths(i) > top5Growths(indexToReplace)) indexToReplace = i
        if (newGrowth > top5Growths(indexToReplace)) return
      }
    }
    top5Growths(indexToReplace) = newGrowth
    top5States(indexToReplace) = newState

  }

  /**
   * find the five extremes of the map
   * finds five largest values if storeMax = true
   * finds five smallest values if storeMax = false
   * @param map
   */
  def findExtremes(map: mutable.Map[String, Double]): Unit = {
    map.foreach(growth => {
      this.insert(growth._1, growth._2)
    })
  }

  def printExtremes(timePeriod: String): Unit = {
    if(storeMax) println(s"Max growth rate of confirmed cases in the US for $timePeriod:")
    else println(s"Min growth rate of confirmed cases in the US for $timePeriod:")
    for(i <- top5Growths.indices) {
      println(s"State: ${top5States(i)} \t Growth Rate: ${top5Growths(i)}")
    }

  }

}