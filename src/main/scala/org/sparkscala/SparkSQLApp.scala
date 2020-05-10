package org.sparkscala

import org.apache.spark.sql.SparkSession

object SparkSQLApp {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName(name="SparkSQLApp").master("local[*]").getOrCreate()



    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    import spark.implicits._
    val customerRDD = spark.sparkContext.parallelize(custs)
    val customerDF = customerRDD.toDF()

    println("*** See the DataFrame contents")
    customerDF.show()

    //
    // Register with a table name for SQL queries
    //
    customerDF.createOrReplaceTempView("customer")

    println("*** Very simple query")
    val allCust = spark.sql("SELECT id, name FROM customer")
    allCust.show()
    println("*** The result has a schema too")
    allCust.printSchema()

    //
    // more complex query: note how it's spread across multiple lines
    //
    println("*** Very simple query with a filter")
    val californiaCust =
      spark.sql(
        s"""
           | SELECT id, name, sales
           | FROM customer
           | WHERE state = 'CA'
         """.stripMargin)
    californiaCust.show()
    californiaCust.printSchema()

    spark.stop()
  }
}
