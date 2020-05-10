package org.sparkscala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object SparkStructuredStreaming {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(name = "SparkStructuredStreamingApp").master("local[*]").getOrCreate()

    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("header", "true")
      .schema(userSchema)      // Specify schema of the csv files
      .csv("src/main/resources/data/csv/")

    val query = csvDF.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start()

    query.awaitTermination()
  }
}
