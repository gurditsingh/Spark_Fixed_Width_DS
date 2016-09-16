package com.spark.datasource.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

/**
 * Created by GURDIT_SINGH on 15-09-2016.
 */
object FixedWidthExample {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()
    val df = sparkSession.read
      .format("com.spark.datasource.fixedwidth")
      .option("length", "1,2,2")
      .schema(StructType(List(StructField("a", IntegerType, nullable = true), StructField("b", IntegerType, nullable = true), StructField("c", StringType, nullable = true))))
      .load("C:\\Java Workspace\\SparkTesting\\input")

    println(df.collectAsList())
  }

}
