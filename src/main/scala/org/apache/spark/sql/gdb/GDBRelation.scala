package org.apache.spark.sql.gdb

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.gdb.core.{GDBRDD, GDBTable}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  */
case class GDBRelation(gdbPath: String, gdbName: String, numPartition: Int)
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation with Logging with TableScan {

  override val schema: StructType = inferSchema()

  private[gdb] def inferSchema(): StructType = {
    val sc = sqlContext.sparkContext
    GDBTable.findTable(gdbPath, gdbName, sc.hadoopConfiguration) match {
      case Some(catTab) => {
        val table = GDBTable(gdbPath, catTab.hexName, sc.hadoopConfiguration)
        try {
          table.schema()
        } finally {
          table.close()
        }
      }
      case _ => {
        log.error(s"Cannot find '$gdbName' in $gdbPath, creating an empty schema !")
        StructType(Seq.empty[StructField])
      }
    }
  }

  override def buildScan(): RDD[Row] = {
    GDBRDD(sqlContext.sparkContext, gdbPath, gdbName, numPartition)
  }
}
