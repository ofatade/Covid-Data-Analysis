package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, isnull, month, not, to_date, udf, when, year}

object dq1 extends Serializable {
  /**
    * Compare state population to number of recovery from April 2020 - April 2021 by Month
    */
  def statePopulationVsDeath(): Unit =
  {
    //Showing the entire file
    println("Showing time_series_covid_19_deaths_US.csv")
    var startTime = System.currentTimeMillis()
    var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
    var transTime = (System.currentTimeMillis() - startTime) / 1000d

    println("Converting INTO Double")
    startTime = System.currentTimeMillis()
    df = df.withColumnRenamed("Admin2", "County")
      .withColumnRenamed("Province_state", "State")
      .withColumnRenamed("5/2/21", "Deaths")
      .withColumn("Population", col("Population").cast("double"))
      .withColumn("Deaths", col("Deaths").cast("double"))
    df = df.select("State","Population","Deaths")
      .orderBy("State")
    println("Showing Only Relevant Columns")
    startTime = System.currentTimeMillis()
    df = df.groupBy(col("State"))
      .sum("Population", "Deaths")

    //Shows Population Ascending
    val ascending = df.orderBy( "sum(Population)" )
    saveDataFrameAsCSV(ascending.limit(10),"bottomTenStates.csv")

    //Shows Population Descending
    val descending =df.orderBy(desc("sum(Population)"))
    saveDataFrameAsCSV(descending.limit(10),"topTenStates.csv")
    df.show(false)

    // Start the Spark session
    System.setProperty("hadoop.home.dir", "C:\\hadoop")  // Assumes this is the location of your Hadoop directory
    Logger.getLogger("org").setLevel(Level.ERROR)  // Hide most of the initial non-error log messages
    spark = SparkSession.builder
      .appName("Proj2")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  // Hide further non-error messages
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    println("Created Spark session.\n")

    // Create the database if needed
    spark.sql("CREATE DATABASE IF NOT EXISTS proj2")
    spark.sql("USE proj2")

    // Run the "statePopulationVsDeath" query
    statePopulationVsDeath()

    // End Spark session
    spark.stop()
    println("Transactions complete.")

  }

}
