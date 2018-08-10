package org.apache.spark.sql.gdb.core

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{BinaryType, Metadata}

/**
  * Created by liyq on 2018/8/9.
  */
class FieldRaster(name: String, nullValueAllowed: Boolean, metadata:Metadata)
  extends FieldBytes(name, BinaryType, nullValueAllowed, metadata){
  override def readValue(byteBuffer: ByteBuffer, oid: Int): Any = null
}
