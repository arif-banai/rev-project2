package net.revature.group5

import scala.io.StdIn
import scala.util.matching.Regex

object Main {

  val commandPatternRegex: Regex = "(\\w+)\\s*(.*)".r

  def main(args: Array[String]): Unit = {
    Spark_App.initialize()

    loop()
  }

  def loop(): Unit = {
    var active = true

    println("Welcome to our COVID-19 Data Analysis Tool")
    println("Please select a query to run")

    while(active) {
      printUserOptions()
      var input = StdIn.readLine()

      input match {
        case commandPatternRegex(cmd, arg) if cmd == "1" => {
          println("Getting most confirmed cases by country...")
          Spark_App.mostConfirmedCases()
        }

        case commandPatternRegex(cmd, arg) if cmd == "2" => {
          println("Getting most deaths by country...")
          Spark_App.mostDeaths()
        }

        case commandPatternRegex(cmd, arg) if cmd == "3" => {
          println("Getting growth rates by US state per month...")
          Spark_App.statesGrowth()
        }

        case commandPatternRegex(cmd, arg) if cmd == "4" => {
          println("Getting various ratios of deaths, confirmed, and recovered by country...")
          Spark_App.topRatio()
        }
1
        case commandPatternRegex(cmd, arg) if cmd == "5" => {
          println("Exiting COVID-19 Analysis Tool...")
          Spark_App.close()
          active = false
        }
      }
    }

    println("End of program execution")
  }

  def printUserOptions(): Unit = {
    List(
      "1: most-confirmed: Get most confirmed cases by country",
      "2: most-deaths: Get most deaths by country",
      "3: states-growth: Get growth rates by US states within 1 month periods",
      "4: top-ratios: Get deaths/confirmed, recov./confirmed, and deaths/recov. by country",
      "5: close: close the application"
    ).foreach(println)
  }
}