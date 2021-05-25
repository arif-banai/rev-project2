package net.revature.group5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.BufferedReader
import scala.io.Source

object Spark_App {

  val br: BufferedReader = Source.fromResource("hdfs-location.txt").bufferedReader();
  val hdfsLocation: String = br.readLine();
  br.close()


  val spark: SparkSession = {
    SparkSession
      .builder
      .appName("Covid-19 Analysis")
      .config("spark.master", "local")
      .config("spark.eventLog.enabled", false)
      .getOrCreate()
  }

  def loadData(): Unit = {
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

    userFile.show()

    userFile.printSchema()
  }

}