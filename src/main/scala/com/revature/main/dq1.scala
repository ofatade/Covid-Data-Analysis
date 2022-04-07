package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.functions.{col, isnull, month, not, to_date, udf, when, year}

object dq1 extends Serializable {
  /**
    * Compare state population to number of recovery from April 2020 - April 2021 by Month
    */
  def statePopVsRecoveryByMonth(): Unit = {
    //import covid_19.csv as dataframe with modified columns
    var df = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load("covid_19_data.csv")
      .drop("SNo", "Last Update", "Confirmed", "Deaths")
      .withColumn("ObservationDate", to_date(col("ObservationDate"), "MM/dd/yyyy"))
      .withColumnRenamed("ObservationDate", "Date")
      .withColumnRenamed("Province/State", "State")
      .withColumnRenamed("Country/Region", "Country")
      .filter(col("Country").like("US"))
      .drop("Country")
      .withColumn("Month", month(col("Date")))
      .withColumn("Year", year(col("Date")))
      .drop("Date")
      .groupBy("State", "Year", "Month")
      .sum("Recovered")
      .orderBy("State", "Month", "Year")

    val statesAbbrev = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load("statesAndAbbreviations.csv")

    var correctStates=df
      .withColumn("RealState",when(col("State").rlike(", [a-z]{2}"), col("State").substr()
      .withColumn("Country", when(col("Country").contains("China"), "China") otherwise (col("Country"))

  }

}
