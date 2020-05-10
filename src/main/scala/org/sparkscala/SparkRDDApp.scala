package org.sparkscala

import org.apache.spark.sql.SparkSession

object SparkRDDApp {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName(name="SparkRDDApp").master("local[*]").getOrCreate()
    val numbers = 1 to 10
    val numbersRDD = spark.sparkContext.parallelize(numbers)
    println("Print each element of the original RDD")
    numbersRDD.foreach(println)
    spark.stop()
  }
}
