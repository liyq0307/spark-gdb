package org.apache.spark.sql.gdb.core

/**
  */
case class IndexInfo(var objectID: Int, var seek: Long) {
  def isSeekable = seek > 0
}
