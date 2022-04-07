package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
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

  //def statePopVsRecoveryByMonth(): Unit = {
    //import covid_19.csv as dataframe with modified columns
    //var df = spark.read.format("csv")
      //.option("header", "true").option("inferSchema", "true")
      //.load("covid_19_data.csv")
      //.drop("SNo", "Last Update", "Confirmed", "Deaths")
      //.withColumn("ObservationDate", to_date(col("ObservationDate"), "MM/dd/yyyy"))
      //.withColumnRenamed("ObservationDate", "Date")
      //.withColumnRenamed("Province/State", "State")
      //.withColumnRenamed("Country/Region", "Country")
      //.filter(col("Country").like("US"))
      //.drop("Country")
      //.withColumn("Month", month(col("Date")))
      //.withColumn("Year", year(col("Date")))
      //.drop("Date")
      //.groupBy("State", "Year", "Month")
      //.sum("Recovered")
      //.orderBy("State", "Month", "Year")

    df.show(false)
    //val statesAbbrev = spark.read.format("csv")
      //.option("header", "true").option("inferSchema", "true")
      //.load("statesAndAbbreviations.csv")

    //var correctStates=df
      //.withColumn("RealState",when(col("State").rlike(", [a-z]{2}"), col("State").substr()
      //.withColumn("Country", when(col("Country").contains("China"), "China") otherwise (col("Country"))

  }

}
