package org.apache.spark.sql.gdb.core

/**
  */
case class IndexInfo(var objectID: Int, var seek: Int) {
  def isSeekable = seek > 0
}
