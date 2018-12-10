package org.apache.spark.sql.gdb.core

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

/**
  */
class GDBRowIterator(indexIter: Iterator[IndexInfo], dataBuffer: DataBuffer, fields: Array[Field], schema: StructType)
  extends Iterator[Row] with Serializable {

  val numFieldsWithNullAllowed = fields.count(_.nullable)
  val nullValueMasks = new Array[Byte]((numFieldsWithNullAllowed / 8.0).ceil.toInt)

  def hasNext() = indexIter.hasNext

  def next() = {
    val index = indexIter.next()
    val numBytes = try {
      dataBuffer.seek(index.seek).readBytes(4).getInt
    } catch {
      case _: Throwable => 0
    }

    val values = if (numBytes == 0) {
      val nullValue: Array[Any] = fields.map(_ => null)
      nullValue
    } else {
      try {
        val byteBuffer = dataBuffer.readBytes(numBytes)
        nullValueMasks.indices foreach (nullValueMasks(_) = byteBuffer.get)
        var bit = 0
        fields.map(field => {
          if (field.nullable) {
            val i = bit >> 3
            val m = 1 << (bit & 7)
            bit += 1
            if ((nullValueMasks(i) & m) == 0) {
              field.readValue(byteBuffer, index.objectID)
            }
            else {
              null // TODO - Do not like null here - but...it is nullable !
            }
          } else {
            field.readValue(byteBuffer, index.objectID)
          }
        })
      } catch {
        case _: Throwable =>
          val nullValue: Array[Any] = fields.map(_ => null)
          nullValue
      }
    }

    new GenericRowWithSchema(values, schema)
  }
}
