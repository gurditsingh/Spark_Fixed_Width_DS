package com.spark.datasource.fixedwidth

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{CreatableRelationProvider, BaseRelation, SchemaRelationProvider, RelationProvider}
import org.apache.spark.sql.types.StructType


/**
 * Created by GURDIT_SINGH on 15-09-2016.
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    createRelation(sqlContext, parameters, null)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    parameters.getOrElse("path", sys.error("path must be specified for data file"))
    parameters.getOrElse("length", sys.error("length must be specified for fixed width file"))
    new FixedWidthRelation(parameters.get("path").get, parameters.get("length").get, schema)(sqlContext)
  }

  private def toIntLength(fieldsLen: String): Array[Int] = {
    val len = fieldsLen.split(",")
    len.map(x => x.toInt)
  }


  def saveAsFW(dataFrame: DataFrame, path: String, fieldsLength: String) = {
    val fieldlen = toIntLength(fieldsLength)
    val valueRDD = dataFrame.rdd.mapPartitions(itr => {
      itr.map(row => {
        if (row.size != fieldlen.length)
          sys.error("row and fields length must be same")
        def getValues(value: String, acc: Int): String = {
          if (acc == row.size)
            return value
          val data = row.get(acc)
          val lengthDiff = data.toString.length - fieldlen(acc)
          println("-----------------" + lengthDiff)
          val result = lengthDiff match {
            case _ if lengthDiff == 0 => data
            case _ if lengthDiff > 0 => data.toString.substring(0, fieldlen(acc))
            case _ if lengthDiff < 0 => {
              var res: String = ""
              var fillerValue = ""
              if (data.isInstanceOf[Number])
                fillerValue = "0";
              else
                fillerValue = " "
              for (i <- 1 to (lengthDiff * -1)) {
                res += fillerValue
              }
              (res + data)
            }
          }
          return getValues(value + result.toString, acc + 1)
        }
        getValues("", 0)
      })
    })
    valueRDD.saveAsTextFile(path)
  }

  def addf(data: String, length: Int, filler: String): String = {
    var res = ""
    for (i <- 1 to length)
      res += filler
    return res + data
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    parameters.getOrElse("path", sys.error("path must be specified"))
    parameters.getOrElse("length", sys.error("length must be specified"))
    val fieldsLength = parameters.get("length").get
    val path = parameters.get("path").get
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    val isSave = if (fs.exists(fsPath)) {
      mode match {
        case SaveMode.Append => sys.error("not suppored")
        case SaveMode.Overwrite =>
          fs.delete(fsPath, true)
          true
        case SaveMode.ErrorIfExists => sys.error("path already exists")
        case SaveMode.Ignore => false
      }
    } else
      true

    if (isSave)
      saveAsFW(data, path, fieldsLength)


    createRelation(sqlContext, parameters, data.schema)
  }
}
