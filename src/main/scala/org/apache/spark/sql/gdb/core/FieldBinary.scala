package org.apache.spark.sql.gdb.core

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{BinaryType, Metadata}

/**
  */
class FieldBinary(name: String, nullValueAllowed: Boolean, metadata:Metadata)
  extends FieldBytes(name, BinaryType, nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    getByteBuffer(byteBuffer)
  }

}
