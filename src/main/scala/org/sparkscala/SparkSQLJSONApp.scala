package org.sparkscala

import org.apache.spark.sql.SparkSession

object SparkSQLJSONApp {

  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName(name="SparkSQLJSONApp").master("local[*]").getOrCreate()

    import spark.implicits._

    // easy enough to query flat JSON
    val people = spark.read.json("src/main/resources/data/flat.json")
    people.printSchema()
    people.createOrReplaceTempView("people")
    val young = spark.sql("SELECT firstName, lastName FROM people WHERE age < 30")
    young.foreach(r => println(r))
    spark.stop()
  }
}
