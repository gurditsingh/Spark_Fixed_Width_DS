package com.spark.datasource.fixedwidth

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{TableScan, BaseRelation}
import org.apache.spark.sql.types.StructType

/**
 * Created by GURDIT_SINGH on 15-09-2016.
 */
class FixedWidthRelation(locatioin:String,fieldslenght:Array[Int],userSchema:StructType)(@transient val sqlContext: SQLContext)extends BaseRelation with TableScan{
  override def schema: StructType = userSchema

  override def buildScan(): RDD[Row] = {
    val fileRDD=sqlContext.sparkContext.textFile(locatioin)
    val rowRDD=fileRDD.map(row=> {
    var parsedLength=0
    var counter:Int=0
    val tokens=new Array[Any](fieldslenght.length)
      for(i <- fieldslenght.length){
        tokens(counter)=row.substring(parsedLength,parsedLength+i)
        parsedLength+=i;
        counter+=1
      }
      Row.fromSeq(tokens)
    })
    rowRDD
  }
}
