package net.revature.group5

import scala.collection.mutable


object Top5 {

  val topElements = new mutable.TreeSet[Double]()

  def main(args: Array[String]): Unit = {
    add(11)
    add(12)
    add(14)
    add(20)
    add(80)
    add(15)
    println(topElements.toString())
  }

  def add(num: Double) {
    if(topElements.size < 5) {
      topElements.add(num)
    } else {
      val first = topElements.min

      if(first < num) {
        topElements.remove(first)
        topElements.add(num)
      }
    }
  }



}
