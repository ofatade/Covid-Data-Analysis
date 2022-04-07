package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.functions.col

object dq2 {
  /**
    * Compare ratio of deaths to recoveries by state from April 2020 - April 2021
    */
  def getUniqueCountries(): Unit = {
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
      .withColumn("Confirmed", col("Confirmed").cast("int"))
      .withColumn("Deaths", col("Deaths").cast("int"))
      .withColumn("Recovered", col("Recovered").cast("int"))
    var df3 = df2.select("State", "ObservationDate", "Recovered")
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    df2.show(false)
    println(s"Table length: ${df.count()}")
    println(s"Transaction time: $transTime seconds")
    df2.printSchema()

    // Copy the dataframe data into table "testdftable"
    println("Table filled from dataframe:")
    startTime = System.currentTimeMillis()
    spark.sql("DROP TABLE IF EXISTS testdftable")
    df2.createOrReplaceTempView("temptable") // Copies the dataframe into a view as "temptable"
    spark.sql("CREATE TABLE testdftable AS SELECT * FROM temptable") // Loads the data into the table from the view
    spark.catalog.dropTempView("temptable") // View no longer needed
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    var tabledat = spark.sql("SELECT * FROM testdftable").orderBy("SNo")
    tabledat.show(false)
    // tabledat.explain()  // Shows the table's definition
    spark.sql("SELECT COUNT(*) FROM testdftable").show()
    println(s"Transaction time: $transTime seconds\n")



    // Create a table of just "State" and "Country" with unique rows
    println("Transformation - Unique locations:")
    startTime = System.currentTimeMillis()
    df = spark.sql("SELECT * FROM testdftable").groupBy("State", "ObservationDate").count().withColumnRenamed("count", "Datapoints").orderBy("ObservationDate")
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    df.show(false)
    println(s"Unique locations: ${df.count()}")
    println(s"Transaction time: $transTime seconds\n")

    // Write the data out as a file to be used for visualization
    println("Save unique locations as file...")
    startTime = System.currentTimeMillis()
    val fname = saveDataFrameAsCSV(df, "uniqueLocations.csv")
    transTime = (System.currentTimeMillis() - startTime) / 1000d
    println(s"Saved as: $fname")
    println(s"Save completed in $transTime seconds.\n")
  }
}
