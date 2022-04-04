package com.revature

import org.apache.spark.sql.functions.col
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ SparkSession, SaveMode, Row, DataFrame }
import org.apache.spark.sql.types._
import org.apache.commons.io.FileUtils
import java.io.File
import util.Try

object Project2 {
	var spark:SparkSession = null

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

	/**
	  * This is a "dummy" query which you can use as example code.
	  */
	private def getUniqueCountries (): Unit = {
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
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		df2.show(false)
		println(s"Table length: ${df.count()}")
		println(s"Transaction time: $transTime seconds")
		df2.printSchema()

		// Copy the dataframe data into table "testdftable"
		println("Table filled from dataframe:")
		startTime = System.currentTimeMillis()
		spark.sql("DROP TABLE IF EXISTS testdftable")
		df2.createOrReplaceTempView("temptable")  // Copies the dataframe into a view as "temptable"
		spark.sql("CREATE TABLE testdftable AS SELECT * FROM temptable")  // Loads the data into the table from the view
		spark.catalog.dropTempView("temptable")  // View no longer needed
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		var tabledat = spark.sql("SELECT * FROM testdftable").orderBy("SNo")
		tabledat.show(false)
		// tabledat.explain()  // Shows the table's definition
		spark.sql("SELECT COUNT(*) FROM testdftable").show()
		println(s"Transaction time: $transTime seconds\n")

		// Create a table of just "State" and "Country" with unique rows
		println("Transformation - Unique locations:")
		startTime = System.currentTimeMillis()
		df = spark.sql("SELECT * FROM testdftable").groupBy("State", "Country").count().withColumnRenamed("count", "Datapoints").orderBy("Country", "State")
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
		getUniqueCountries()

		// End Spark session
		spark.stop()
		println("Transactions complete.")
	}
}
