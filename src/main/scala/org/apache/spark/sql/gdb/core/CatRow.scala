package org.apache.spark.sql.gdb.core

/**
  * Catalog Row
  */
case class CatRow(id: Int, name: String) {
  val hexName = "a%08x".format(id)
}
