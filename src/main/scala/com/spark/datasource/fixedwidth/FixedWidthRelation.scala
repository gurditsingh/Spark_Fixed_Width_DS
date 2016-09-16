package com.spark.datasource.fixedwidth

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{ BaseRelation, TableScan }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType

/**
 * Created by GURDIT_SINGH
 */
class FixedWidthRelation(location: String, fieldslength: String, userSchema: StructType)
(@transient val sqlContext: SQLContext) 
extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    if (this.userSchema == null)
      throw new RuntimeException
    userSchema

  }

  override def buildScan(): RDD[Row] = {
    val fileRDD = sqlContext.sparkContext.textFile(location)
    val schemafields=schema.fields
    val rowRDD = fileRDD.map(row => {
      def getValue(fieldsLength: List[Int], acc: Int,counter:Int): List[Any] = fieldsLength match {
        case List() => List()
        case x :: xs =>
          val fieldValue=toCast(row.substring(acc, acc + x), schemafields(counter).dataType)
          List(fieldValue) ++ getValue(xs, acc + x,counter+1)
      }
      Row.fromSeq(getValue(toIntLength(fieldslength), 0,0))
    })
    rowRDD
  }

  private def toIntLength(fieldsLen: String): List[Int] = {
    val len = fieldsLen.split(",")
    len.map(x => x.toInt).toList
  }
  
  private def toCast(value:String,dataType:DataType)={
    dataType match{
      case _:StringType=> value
      case _:IntegerType=>value.toInt
    }
    
  }
}
