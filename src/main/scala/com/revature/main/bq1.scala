package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import com.revature.main.tq2.t2020
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, isnull, lag, lit, month, not, regexp_replace, to_date, when}

object bq1 {
    var tempsHistAvg:DataFrame=null
    var covidAvgDaily:DataFrame=null
    var populationNormalization:DataFrame=null
    var joined:DataFrame=null
  /**
    * Creates a .csv file entitled '30yrAvgTempByCountryByMonth.csv', that contains historical average (1982-2012) temperature of country by month
    */
  def normalizePopulation(): Unit = {
    // Read "covid_19_data.csv" data as a dataframe
    println("Dataframe read from CSV:")
    var startTime = System.currentTimeMillis()
    var raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("population_by_country_2020.csv")
    var df=raw.select("Country", "Population", "WorldShare")
      .withColumnRenamed("Country (or dependency)", "Country")
      .withColumnRenamed("World Share", "WorldShare")
      .withColumn("WorldShare", regexp_replace(col("WorldShare"),"%",""))
    df.show
    //drop all rows containing any null value
    raw = raw.na.drop()
    var df = raw.filter(raw("Date").gt(lit("1982-01-01"))).filter(raw("Date").lt(lit("2012-12-31")))
    //add month column
    df = df.withColumn("Month", month(col("Date")))
    df = df.groupBy("Country", "Month")
      .avg("AverageTemperature", "AverageTemperatureUncertainty")
      .orderBy("Country", "Month")
    var transTime = (System.currentTimeMillis() - startTime) / 1000d
    println(s"Table length: ${df.count()}")
    println(s"Transaction time: $transTime seconds")
    df.printSchema()
    // Write the data out as a file to be used for visualization
    tempsHistAvg=df
    println("Save Historical Temperature Average by Country by Month as file...")
    startTime = System.currentTimeMillis()
    val fname = saveDataFrameAsCSV(df, "30yrAvg1982-2012TempByCountryByMonth.csv")
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    println(s"Saved as: $fname")
    println(s"Save completed in $transTime seconds.\n")
  }


    /**
      * Creates a .csv file entitled '30yrAvgTempByCountryByMonth.csv', that contains historical average (1982-2012) temperature of country by month
      */
    def createHistoricalTemperatureAverageByCountryByMonth(): Unit = {
      // Read "covid_19_data.csv" data as a dataframe
      println("Dataframe read from CSV:")
      var startTime = System.currentTimeMillis()
      var raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
        .load("GlobalLandTemperaturesByCountry.csv").withColumn("dt", to_date(col("dt")))
        .withColumnRenamed("dt", "Date")
      //drop all rows containing any null value
      raw = raw.na.drop()
      var df = raw.filter(raw("Date").gt(lit("1982-01-01"))).filter(raw("Date").lt(lit("2012-12-31")))
      //add month column
      df = df.withColumn("Month", month(col("Date")))
      df = df.groupBy("Country", "Month")
        .avg("AverageTemperature", "AverageTemperatureUncertainty")
        .orderBy("Country", "Month")
      var transTime = (System.currentTimeMillis() - startTime) / 1000d
      println(s"Table length: ${df.count()}")
      println(s"Transaction time: $transTime seconds")
      df.printSchema()
      // Write the data out as a file to be used for visualization
      tempsHistAvg=df
      println("Save Historical Temperature Average by Country by Month as file...")
      startTime = System.currentTimeMillis()
      val fname = saveDataFrameAsCSV(df, "30yrAvg1982-2012TempByCountryByMonth.csv")
      transTime = (System.currentTimeMillis() - startTime) / 1000d
      println(s"Saved as: $fname")
      println(s"Save completed in $transTime seconds.\n")
    }

    /**
      * TODO
      */
    def calcAvgDailyConfirmedDeathsRecovered(): Unit = {
      // Read "covid_19_data.csv" data as a dataframe
      println("Dataframe read from CSV:")
      var startTime = System.currentTimeMillis()
      var raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
        .load("covid_19_data.csv")
        .withColumn("ObservationDate", to_date(col("ObservationDate"), "MM/dd/yyyy"))
        .withColumnRenamed("ObservationDate", "Date")
        .withColumnRenamed("Country/Region", "Country")
      var df = raw.filter(not(col("Country").like("China")))
        .withColumn("Country", when(col("Country").contains("China"), "China") otherwise (col("Country")))

      df.show()
      df = df.withColumnRenamed("sum(Confirmed)", "Confirmed")
        .withColumnRenamed("sum(Deaths)", "Deaths")
        .withColumnRenamed("sum(Recovered)", "Recovered")
      val rate_window = Window.partitionBy("Country").orderBy("Country")
      df = df.withColumn("confirmed_prev_value", lag(col("Confirmed"), 1).over(rate_window))
      df = df.withColumn("DailyConfirmed", when(isnull(col("confirmed") - col("confirmed_prev_value")), 0)
        .otherwise(col("confirmed") - col("confirmed_prev_value")))
      df = df.withColumn("deaths_prev_value", lag(col("Deaths"), 1).over(rate_window))
      df = df.withColumn("DailyDeaths", when(isnull(col("Deaths") - col("deaths_prev_value")), 0)
        .otherwise(col("deaths") - col("deaths_prev_value")))
      df = df.withColumn("recovered_prev_value", lag(col("recovered"), 1).over(rate_window))
      df = df.withColumn("DailyRecovered", when(isnull(col("recovered") - col("recovered_prev_value")), 0)
        .otherwise(col("recovered") - col("recovered_prev_value")))
      //add month column
      df = df.withColumn("Month", month(col("Date")))
      df = df.groupBy(col("Country"), col("Month"))
        .avg("DailyConfirmed", "DailyDeaths", "DailyRecovered")
        .orderBy("Country", "Month")
      df = df.select(col("Country"),
        col("Month"),
        col("avg(DailyConfirmed)").alias("AvgDailyConfirmed"),
        col("avg(DailyDeaths)").alias("AvgDailyDeaths"),
        col("avg(DailyRecovered)").alias("AvgDailyRecovered"))
      var transTime = (System.currentTimeMillis() - startTime) / 1000d
      println(s"Table length: ${df.count()}")
      println(s"Transaction time: $transTime seconds")
      df.printSchema()
      // Write the data out as a file to be used for visualization
      covidAvgDaily=df
      println("Save Avg_Daily_Confirmed_Deaths_Recovered as file...")
      startTime = System.currentTimeMillis()
      val fname = saveDataFrameAsCSV(df, "AvgDailyConfirmedDeathsRecovered.csv")
      transTime = (System.currentTimeMillis() - startTime) / 1000d
      println(s"Saved as: $fname")
      println(s"Save completed in $transTime seconds.\n")
    }

    /**
      * TODO
      */
    def joinAndOutputRatesWithTemperature(): Unit = {
      joined = covidAvgDaily.join(tempsHistAvg, usingColumns = Seq("Country", "Month"))
        .where((covidAvgDaily("Country") === tempsHistAvg("Country")) and ((covidAvgDaily("Month") === tempsHistAvg("Month"))))
      // Write the data out as a file to be used for visualization
      val startTime = System.currentTimeMillis()
      val fname = saveDataFrameAsCSV(joined, "AvgDailyRates&Temperature_ByCountryByTemperature.csv")
      val transTime = (System.currentTimeMillis() - startTime) / 1000d
      println(s"Saved as: $fname")
      println(s"Save completed in $transTime seconds.\n")
    }
}
