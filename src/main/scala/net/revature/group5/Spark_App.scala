package net.revature.group5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import java.io.BufferedReader
import scala.collection.mutable
import scala.io.Source

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

    val statesGrowth = spark.sql("SELECT `Province_State` AS states, cast(growthPercentage(`Apr_2020`, `May_2020`) AS decimal(10,2)) as growth_April_May, " +
      "cast(growthPercentage(`May_2020`, `June_2020`) AS decimal(10,2)) as growth_May_June, " +
      "cast(growthPercentage(`June_2020`, `July_2020`) AS decimal(10,2)) as growth_June_July, " +
      "cast(growthPercentage(`July_2020`, `August_2020`) AS decimal(10,2)) as growth_July_August, " +
      "cast(growthPercentage(`August_2020`, `September_2020`) AS decimal(10,2)) as growth_August_September, " +
      "cast(growthPercentage(`September_2020`, `October_2020`) AS decimal(10,2)) as growth_September_October, " +
      "cast(growthPercentage(`October_2020`, `November_2020`) AS decimal(10,2)) as growth_October_November, " +
      "cast(growthPercentage(`November_2020`, `December_2020`) AS decimal(10,2)) as growth_November_December, " +
      "cast(growthPercentage(`December_2020`, `January_2021`) AS decimal(10,2)) as growth_December_January, " +
      "cast(growthPercentage(`January_2021`, `February_2021`) AS decimal(10,2)) as growth_January_February, " +
      "cast(growthPercentage(`February_2021`, `March_2021`) AS decimal(10,2)) as growth_February_March, " +
      "cast(growthPercentage(`March_2021`, `April_2021`) AS decimal(10,2)) as growth_March_April, " +
      "cast(growthPercentage(`April_2021`, `May_2021`) AS decimal(10,2)) as growth_April_May " +
      "FROM statesMonths ORDER BY states")

    statesGrowth.show()
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
