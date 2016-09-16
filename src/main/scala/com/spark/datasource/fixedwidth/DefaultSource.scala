package com.spark.datasource.fixedwidth

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider, RelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Created by GURDIT_SINGH on 15-09-2016.
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
  createRelation(sqlContext,parameters,null)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    parameters.getOrElse("path",sys.error("path must be specified for data file"))
    parameters.getOrElse("length",sys.error("length must be specified for fixed width file"))
    new FixedWidthRelation(parameters.get("path").get,parameters.get("length").get,schema)(sqlContext)
  }
  
  
}
