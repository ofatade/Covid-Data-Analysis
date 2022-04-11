package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.functions.col

object dq2 {
  /**
    * Compare ratio of deaths to recoveries by state from April 2020 - April 2021
    */
  def countryCasesVsDeath(): Unit = {
    // Read "covid_19_data.csv" data as a dataframe
    println("Dataframe read from CSV:")
    var startTime = System.currentTimeMillis()
    var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("covid_19_data.csv")
    var transTime = (System.currentTimeMillis() - startTime) / 1000d
    df.show(false)
    println(s"Table length: ${df.count()}")
    println(s"Transaction time: $transTime seconds")
    df.printSchema()

    // Modify dataframe to change column names and cast doubles to ints
    println("Modified dataframe:")
    startTime = System.currentTimeMillis()
    var df2 = df.withColumnRenamed("Province/State", "State")
      .withColumnRenamed("Country/Region", "Country")
      .withColumnRenamed("Confirmed", "Cases")
      .withColumn("Confirmed", col("Confirmed").cast("int"))
      .withColumn("Deaths", col("Deaths").cast("int"))
      .withColumn("Recovered", col("Recovered").cast("int"))
    var df3 = df2.select("State", "ObservationDate", "Recovered")
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    df2.show(false)
    println(s"Table length: ${df.count()}")
    println(s"Transaction time: $transTime seconds")
    df2.printSchema()


  }
}
