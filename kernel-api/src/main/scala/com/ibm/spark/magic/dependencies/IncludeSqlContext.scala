package com.ibm.spark.magic.dependencies

//dt - Added this trait

import com.ibm.spark.magic.Magic
import org.apache.spark.sql.SQLContext

trait IncludeSqlContext {
  this: Magic =>

  //val sqlContext: SqlContext
  private var _sqlContext: SQLContext = _
  def sqlContext: SQLContext = _sqlContext
  def sqlContext_=(newSqlContext: SQLContext) =
    _sqlContext = newSqlContext
}