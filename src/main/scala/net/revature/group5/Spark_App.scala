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

    (fileNames, keyNames).zipped.foreach( (fileName, keyName) => {
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
