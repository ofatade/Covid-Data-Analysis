package com.revature

import org.apache.spark.sql.functions.{col, isnull, lag, lit, month, not, to_date, when}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.catalyst.expressions.Month
import org.apache.spark.sql.expressions.Window

import java.text.SimpleDateFormat
import java.io.File
import util.Try

object Project2 {
	var spark:SparkSession = null

	def date_format(value: Any, str: String) = ???

	/**
	  * Creates a .csv file entitled '30yrAvgTempByCountryByMonth.csv', that contains historical average (1982-2012) temperature of country by month
	  */
	private def createHistoricalTemperatureAverageByCountryByMonth(): Unit = {
		// Read "covid_19_data.csv" data as a dataframe
		println("Dataframe read from CSV:")
		var startTime = System.currentTimeMillis()
		var raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
			.load("GlobalLandTemperaturesByCountry.csv").withColumn("dt", to_date(col("dt")))
			.withColumnRenamed("dt","Date")
		//drop all rows containing any null value
		raw = raw.na.drop()
		var df = raw.filter(raw("Date").gt(lit("1982-01-01"))).filter(raw("Date").lt(lit("2012-12-31")))
		//add month column
		df= df.withColumn("Month",month(col("Date")))

		df=df.groupBy("Country","Month").avg("AverageTemperature","AverageTemperatureUncertainty").orderBy("Country","Month")
		var transTime = (System.currentTimeMillis() - startTime) / 1000d
		df.show(false)
		println(s"Table length: ${df.count()}")
		println(s"Transaction time: $transTime seconds")
		df.printSchema()
		// Write the data out as a file to be used for visualization
		println("Save Historical Temperature Average by Country by Month as file...")
		startTime = System.currentTimeMillis()
		val fname = saveDataFrameAsCSV(df, "30yrAvgTempByCountryByMonth.csv")
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		println(s"Saved as: $fname")
		println(s"Save completed in $transTime seconds.\n")
	}
	/**
		* TODO
		*/
	private def calcAvgDailyConfirmedDeathsRecovered(): Unit = {
		// Read "covid_19_data.csv" data as a dataframe
		println("Dataframe read from CSV:")
		var startTime = System.currentTimeMillis()
		var raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
			.load("covid_19_data.csv").withColumn("ObservationDate", to_date(col("ObservationDate"),"MM/dd/yyyy"))
			.withColumnRenamed("ObservationDate","Date").withColumnRenamed("Country/Region","Country")
		var df = raw.filter(not(col("Country").like("China"))).withColumn("Country", when(col("Country").contains("China"), "China")otherwise(col("Country")))
		val rate_window=Window.partitionBy("Country").orderBy("Country")
		df = df.withColumn("confirmed_prev_value", lag(col("Confirmed"),1).over(rate_window))
		df = df.withColumn("DailyConfirmed", when(isnull(col("confirmed") - col("confirmed_prev_value")) ,0)
			.otherwise(col("confirmed") - col("confirmed_prev_value")))
		df = df.withColumn("deaths_prev_value", lag(col("Deaths"),1).over(rate_window))
		df = df.withColumn("DailyDeaths", when(isnull(col("Deaths") - col("deaths_prev_value")) ,0)
			.otherwise(col("deaths") - col("deaths_prev_value")))
		df = df.withColumn("recovered_prev_value", lag(col("recovered"),1).over(rate_window))
		df = df.withColumn("DailyRecovered", when(isnull(col("recovered") - col("recovered_prev_value")) ,0)
			.otherwise(col("recovered") - col("recovered_prev_value")))
		//add month column
		df= df.withColumn("Month",month(col("Date")))
		df=df.groupBy(col("Country"),col("Month")).avg("DailyConfirmed","DailyDeaths","DailyRecovered")
			.orderBy("Country","Month")
		df=df.select(col("Country"),
			col("Month"),
			col("avg(DailyConfirmed)").alias("AvgDailyConfirmed"),
			col("avg(DailyDeaths)").alias("AvgDailyDeaths"),
			col("avg(DailyRecovered)").alias("AvgDailyRecovered"))
		var transTime = (System.currentTimeMillis() - startTime) / 1000d
		df.show(false)
		println(s"Table length: ${df.count()}")
		println(s"Transaction time: $transTime seconds")
		df.printSchema()
		// Write the data out as a file to be used for visualization
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
	def joinAndOutputRatesWithTemperature():Unit = {
		println("Dataframe(s) read from CSV:")
		var startTime = System.currentTimeMillis()
		val covid = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
			.load("AvgDailyConfirmedDeathsRecovered.csv")
		val temps = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
			.load("30yrAvgTempByCountryByMonth.csv")
		var joined = covid.join(temps, usingColumns = Seq("Country", "Month")).where((covid("Country") === temps("Country")) and ((covid("Month") === temps("Month"))))
		joined.show()
		// Write the data out as a file to be used for visualization
		var transTime = (System.currentTimeMillis() - startTime) / 1000d
		println(s"Transaction time: $transTime seconds")
		startTime = System.currentTimeMillis()
		val fname = saveDataFrameAsCSV(joined, "CovidRates&Temperature_ByCountryByTemperature.csv")
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		println(s"Saved as: $fname")
		println(s"Save completed in $transTime seconds.\n")
	}

	/**
	  * Main program section.  Sets up Spark session, runs queries, and then closes the session.
	  *
	  * @param args	Executable's paramters (ignored).
	  */
	def main (args: Array[String]): Unit = {

		// Start the Spark session
		System.setProperty("hadoop.home.dir", "C:\\hadoop")
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
		// Run the "getUniqueCountries" query
		createHistoricalTemperatureAverageByCountryByMonth()
		calcAvgDailyConfirmedDeathsRecovered()
		joinAndOutputRatesWithTemperature()

		// End Spark session
		spark.stop()
		println("Transactions complete.")
	}

	/**
		* Gets a list of filenames in the given directory, filtered by optional matching file extensions.
		*
		* @param dir			Directory to search.
		* @param extensions	Optional list of file extensions to find.
		* @return				List of filenames with paths.
		*/
	def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
		dir.listFiles.filter(_.isFile).toList.filter { file => extensions.exists(file.getName.endsWith(_)) }
	}

	/**
		* Moves/renames a file.
		*
		* @param oldName	Old filename and path.
		* @param newName	New filename.
		* @return			Success or failure.
		*/
	def mv(oldName: String, newName: String) = {
		Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
	}

	def saveDataFrameAsCSV(df: DataFrame, filename: String): String = {
		df.coalesce(1).write.options(Map("header"->"true", "delimiter"->",")).mode(SaveMode.Overwrite).format("csv").save("tempCSVDir")
		val curDir = System.getProperty("user.dir")
		val srcDir = new File(curDir + "/tempCSVDir")
		val files = getListOfFiles(srcDir, List("csv"))
		var srcFilename = files(0).toString()
		val destFilename = curDir + "/" + filename
		FileUtils.deleteQuietly(new File(destFilename))  // Clear out potential old copies
		mv(srcFilename, destFilename)  // Move and rename file
		FileUtils.deleteQuietly(srcDir)  // Delete temp directory
		destFilename
	}



}
