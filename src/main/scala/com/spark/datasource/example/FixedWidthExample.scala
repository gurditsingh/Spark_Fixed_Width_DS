package com.spark.datasource.example

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructType, IntegerType, StructField, StringType}

/**
 * Created by GURDIT_SINGH
 */
object FixedWidthExample {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    val df = sparkSession.read
      .schema(StructType(List(StructField("a", IntegerType, nullable = true), StructField("b", StringType, nullable = true), StructField("c", StringType, nullable = true))))
      .csv(args(0))

    df.write.format(args(1)).option("length", "3,2,2").mode(SaveMode.Overwrite).save(args(2))

  }

}
