package com.spark.datasource.example

/**
 * Created by GURDIT_SINGH on 15-09-2016.
 */
object FixedWidthExample {

  def main(args: Array[String]) {
//    val
// sc = new SparkContext(args(0), "Csv loading example")
//    val sqlContext = new SQLContext(sc)
//    val fieldLength=Array(2,3)
//    val df = sqlContext.load("com.madhukaraphatak.spark.datasource.csv", Map("path" -> args(1), "header" -> "true"))
//    println(df.count())
//    println(df.collectAsList())

    val value="abcdef"
    println(value.substring(0,2))
  }

}
