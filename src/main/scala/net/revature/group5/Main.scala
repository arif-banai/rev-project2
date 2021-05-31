package net.revature.group5

object Main {
  def main(args: Array[String]): Unit = {
    Spark_App.initialize()
    //Spark_App.mostConfirmedCases()
    //Spark_App.mostDeaths()
    //Spark_App.mostRecovered()

    //Spark_App.topRatio()

    Spark_App.statesGrowth()

    Spark_App.close()
  }
}