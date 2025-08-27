package org.example

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    // ✅ Create SparkSession
    val spark = SparkSession.builder()
      .appName("Employee CSV Reader")
      .master("local[*]")   // run in local mode with all cores
      .getOrCreate()

    // ✅ Read CSV file (with header, infer schema)
    var empDF = spark.read
      .option("header", "true")        // first row has column names
      .option("inferSchema", "true")   // infer column data types
      .csv("src/main/resources/Employee.csv")

    empDF = empDF.na.drop("all")
    // ✅ Show data
    empDF.show(10)

    // ✅ Print schema
    empDF.printSchema()

    // Stop Spark
    spark.stop()
  }
}
