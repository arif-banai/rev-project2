package net.revature.group5

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io.BufferedReader
import scala.collection.mutable
import scala.io.Source

case class provState(Province_State: String, Apr_May20: Double, May_Jun20: Double, Jun_Jul20: Double,
                     Jul_Aug20: Double, Aug_Sep20: Double, Sep_Oct20: Double, Oct_Nov20: Double,
                     Nov_Dec20: Double, Dec_Jan21: Double, Jan_Feb21: Double, Feb_Mar21: Double,
                     Mar_Apr21: Double, Apr_May21: Double)

object Spark_App {

  val br: BufferedReader = Source.fromResource("hdfs-location.txt").bufferedReader();
  val hdfsLocation: String = br.readLine();
  br.close()

  val dataFiles: mutable.Map[String, DataFrame] = scala.collection.mutable.Map[String, DataFrame]()

  val spark: SparkSession = {
    SparkSession
      .builder
      .appName("Covid-19 Analysis")
      .config("spark.master", "local")
      .config("spark.eventLog.enabled", false)
      .getOrCreate()
  }

  def close(): Unit = {
    spark.close()
  }

  def initialize(): Unit = {
    loadCovidData()
    loadTimeSeriesFiles()
  }

  def mostConfirmedCases(): Unit = {

    dataFiles("confirmed").createOrReplaceTempView("confirmed")
    val topConfirmedCases = spark.sql("SELECT `Country/Region` AS country, `5/2/21` AS confirmedCases FROM confirmed ORDER BY confirmedCases DESC")

    topConfirmedCases.show(20)
  }

  def mostDeaths(): Unit = {

    dataFiles("deaths").createOrReplaceTempView("deaths")
    val topConfirmedCases = spark.sql("SELECT `Country/Region` AS country, `5/2/21` AS deadPeople FROM deaths ORDER BY deadPeople DESC")

    topConfirmedCases.show(20)
  }

  def statesGrowth(): Unit = {
    dataFiles("confirmed_US").createOrReplaceTempView("confirmed_US")
    val statesMonths = spark.sql("SELECT `Province_State`, SUM(`2/1/20`) AS Feb_2020, SUM(`3/1/20`) AS Mar_2020, " +
      "SUM(`4/1/20`) AS Apr_2020, SUM(`5/1/20`) AS May_2020, SUM(`6/1/20`) AS June_2020, " +
      "SUM(`7/1/20`) AS July_2020, SUM(`8/1/20`) AS August_2020, SUM(`9/1/20`) AS September_2020, " +
      "SUM(`10/1/20`) AS October_2020, SUM(`11/1/20`) AS November_2020, SUM(`12/1/20`) AS December_2020, " +
      "SUM(`1/1/21`) AS January_2021, SUM(`2/1/21`) AS February_2021, SUM(`3/1/21`) AS March_2021, " +
      "SUM(`4/1/21`) AS April_2021, SUM(`5/1/21`) AS May_2021 " +
      "FROM confirmed_US GROUP BY `Province_State`")

    spark.udf.register("growthPercentage", (first: Double, second: Double) => {
      if (first == 0) {
        0
      } else {
        ((second - first) / Math.abs(first)) * 100.0
      }
    }
    )

    statesMonths.createOrReplaceTempView("statesMonths")

    //cast(growthPercentage(`Apr_2020`, `May_2020`) AS decimal(10,2)) as Apr_May20
    // this is what we did before: cannot up cast from decimal to double
    val statesGrowth = spark.sql("SELECT `Province_State`, " +
      "growthPercentage(`Apr_2020`, `May_2020`) as Apr_May20, " +
      "growthPercentage(`May_2020`, `June_2020`) as May_Jun20, " +
      "growthPercentage(`June_2020`, `July_2020`) as Jun_Jul20, " +
      "growthPercentage(`July_2020`, `August_2020`) as Jul_Aug20, " +
      "growthPercentage(`August_2020`, `September_2020`) as Aug_Sep20, " +
      "growthPercentage(`September_2020`, `October_2020`) as Sep_Oct20, " +
      "growthPercentage(`October_2020`, `November_2020`) as Oct_Nov20, " +
      "growthPercentage(`November_2020`, `December_2020`) as Nov_Dec20, " +
      "growthPercentage(`December_2020`, `January_2021`) as Dec_Jan21, " +
      "growthPercentage(`January_2021`, `February_2021`) as Jan_Feb21, " +
      "growthPercentage(`February_2021`, `March_2021`) as Feb_Mar21, " +
      "growthPercentage(`March_2021`, `April_2021`) as Mar_Apr21, " +
      "growthPercentage(`April_2021`, `May_2021`) as Apr_May21 " +
      "FROM statesMonths ORDER BY Province_State")


    import spark.implicits._
    val ds = statesGrowth.as[provState]

    ds.foreach(state => {
      println(state.Province_State)
    })


  }


  def loadCovidData(): Unit = {
    val covid19dataSchema = StructType {
      Array(
        StructField("SNo", IntegerType),
        StructField("ObservationDate", DateType),
        StructField("Province/State", StringType),
        StructField("Country/Region", StringType),
        StructField("Last Update", StringType),
        StructField("Confirmed", DoubleType),
        StructField("Deaths", DoubleType),
        StructField("Recovered", DoubleType),
      )
    }

    //.option("inferSchema", "true")
    // May work for some files, however, the date fields are being cast to Strings instead of Dates
    val userFile = spark.read
      .option("header", "true")
      .option("sep", ",")
      .option("dateFormat", "MM/dd/yyyy")
      .option("nullValue", "")
      .schema(covid19dataSchema)
      .csv(hdfsLocation + "covid_19_data.csv")
      .toDF()

    dataFiles("covidData") = userFile
  }

  def loadTimeSeriesFiles(): Unit = {

    val fileNames = Array(
      "time_series_covid_19_confirmed.csv",
      "time_series_covid_19_confirmed_US.csv",
      "time_series_covid_19_deaths.csv",
      "time_series_covid_19_deaths_US.csv",
      "time_series_covid_19_recovered.csv"
    )

    val keyNames = Array(
      "confirmed",
      "confirmed_US",
      "deaths",
      "deaths_US",
      "recovered"
    )

    (fileNames, keyNames).zipped.foreach((fileName, keyName) => {
      dataFiles(keyName) = loadDataFile(fileName)
    })

    //    //Test
    //    dataFiles.foreach( pair => {
    //      println(pair)
    //    })
  }

  def loadDataFile(fileName: String): DataFrame = {

    val userFile: DataFrame = spark.read
      .option("header", "true")
      .option("sep", ",")
      .option("dateFormat", "MM/dd/yyyy")
      .option("nullValue", "")
      .option("inferSchema", "true")
      .csv(hdfsLocation + fileName)
      .toDF()

    userFile
  }

}
