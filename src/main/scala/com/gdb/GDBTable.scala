package com.gdb

import java.io.File
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.apache.spark.sql.udt.Logging

import scala.collection.mutable.ArrayBuffer

class GDBTable(dataBuffer: DataBuffer,
               val numRows: Int,
               val geometryType: Int,
               val fields: Array[Field]
              ) extends Logging with AutoCloseable with Serializable {

  def schema() = StructType(fields)

  def rowIterator(index: GDBIndex, startAtRow: Int = 0, numRowsToRead: Int = -1) = {
    // log.info(s"rowIterator::startAtRow=$startAtRow numRowsToRead=$numRowsToRead")
    new GDBRowIterator(index.iterator(startAtRow, numRowsToRead), dataBuffer, fields, schema)
  }

  def seekIterator(indexIter: Iterator[IndexInfo]) = {
    val numFieldsWithNullAllowed = fields.count(_.nullable)
    if (numFieldsWithNullAllowed == 0)
      new GDBTableSeekWithNoNullValues(dataBuffer, fields, indexIter)
    else
      new GDBTableSeekWithNullValues(dataBuffer, fields, numFieldsWithNullAllowed, indexIter)
  }

  // TODO - add Iterator for no null values
  def scanIterator(seek: Int, count: Int = -1, startID: Int = 0) = {
    dataBuffer.seek(seek)
    val maxRows = if (count == -1) numRows else count
    new GDBTableScanWithNullValues(dataBuffer, fields, maxRows, startID)
  }

  def close() {
    dataBuffer.close()
  }

}

object GDBTable extends Logging with Serializable {
  def apply(path: String, name: String, conf: Configuration = new Configuration()) = {
    val filename = StringBuilder.newBuilder.append(path).append(File.separator).append(name).append(".gdbtable").toString()
    val hdfsPath = new Path(filename)
    val dataBuffer = DataBuffer(hdfsPath.getFileSystem(conf).open(hdfsPath))
    val numRows = readHeader(dataBuffer)

    val bb1 = dataBuffer.readBytes(4 + 4 + 4 + 2)
    val numBytes = bb1.getInt - 10
    val i1 = bb1.getInt // Seems to be 3 for FGDB 9.X files and 4 for FGDB 10.X files
    val geometryType = bb1.get
    val b2 = bb1.get
    val b3 = bb1.get
    val geometryProp = bb1.get & 0xFF // 0x40 for geometry with M, 0x80 for geometry with Z
    val numFields = bb1.getShort

    // println(s"gdbType=$i1 $b2 $b3 $geometryProp")

    val bb2 = dataBuffer.readBytes(numBytes)

    val fields = 0 until numFields map (_ => {
      val nameLen = bb2.get
      val name = ((0 until nameLen).foldLeft(new StringBuilder(nameLen))((sb, _) => {
        sb.append(bb2.getChar)
      })).toString

      val aliasLen = bb2.get
      val aliasTemp = ((0 until aliasLen).foldLeft(new StringBuilder(aliasLen))((sb, _) => {
        sb.append(bb2.getChar)
      })).toString
      val alias = if (aliasTemp.isEmpty) name else aliasTemp

      // println(s"$name $alias")

      val fieldType = bb2.get
      fieldType match {
        case GDBFieldType.INT16 => toFieldInt16(bb2, name, alias)
        case GDBFieldType.INT32 => toFieldInt32(bb2, name, alias)
        case GDBFieldType.FLOAT32 => toFieldFloat32(bb2, name, alias)
        case GDBFieldType.FLOAT64 => toFieldFloat64(bb2, name, alias)
        case GDBFieldType.DATETIME => toFieldDateTime(bb2, name, alias)
        case GDBFieldType.STRING => toFieldString(bb2, name, alias)
        case GDBFieldType.OID => toFieldOID(bb2, name, alias)
        case GDBFieldType.SHAPE => toFieldGeom(bb2, name, alias, geometryType, geometryProp)
        case GDBFieldType.BINARY => toFieldBinary(bb2, name, alias)
        case GDBFieldType.UUID | GDBFieldType.GUID => toFieldUUID(bb2, name, alias)
        case GDBFieldType.XML => toFieldXML(bb2, name, alias)
        case _ => throw new RuntimeException(s"Field type $fieldType is not supported")
      }
    })
    new GDBTable(dataBuffer, numRows, geometryType, fields.toArray)
  }

  private def readHeader(dataBuffer: DataBuffer) = {
    val bb = dataBuffer.readBytes(40)
    val signature = bb.getInt // TODO - throw exception if not correct signature
    val numRows = bb.getInt // num rows
    val h2 = bb.getInt
    val h3 = bb.getInt
    val h4 = bb.getInt
    val h5 = bb.getInt
    val fs = bb.getInt // file size - can be negative - maybe uint ?
    val h7 = bb.getInt
    val h8 = bb.getInt
    val h9 = bb.getInt
    dataBuffer.seek(h8)
    numRows
  }

  private def toFieldFloat64(bb: ByteBuffer, name: String, alias: String): Field = {
    val len = bb.get
    val flag = bb.get
    val mask = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .build()
    new FieldFloat64(name, (flag & 1) == 1, metadata)
  }

  private def toFieldFloat32(bb: ByteBuffer, name: String, alias: String): Field = {
    val len = bb.get
    val flag = bb.get
    val mask = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .build()
    new FieldFloat32(name, (flag & 1) == 1, metadata)
  }

  private def toFieldInt16(bb: ByteBuffer, name: String, alias: String): Field = {
    val len = bb.get
    val flag = bb.get
    val mask = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .build()
    new FieldInt16(name, (flag & 1) == 1, metadata)
  }

  private def toFieldInt32(bb: ByteBuffer, name: String, alias: String): Field = {
    val len = bb.get
    val flag = bb.get
    val mask = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .build()
    new FieldInt32(name, (flag & 1) == 1, metadata)
  }

  private def toFieldBinary(bb: ByteBuffer, name: String, alias: String): Field = {
    val len = bb.get
    val flag = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .build()
    new FieldBinary(name, (flag & 1) == 1, metadata)
  }

  private def toFieldUUID(bb: ByteBuffer, name: String, alias: String): Field = {
    val len = bb.get
    val flag = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .build()
    new FieldUUID(name, (flag & 1) == 1, metadata)
  }

  private def toFieldXML(bb: ByteBuffer, name: String, alias: String): Field = {
    val len = bb.get
    val flag = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .build()
    new FieldString(name, (flag & 1) == 1, metadata)
  }

  private def toFieldString(bb: ByteBuffer, name: String, alias: String): Field = {
    val maxLen = bb.getInt
    val flag = bb.get
    val mask = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("maxLength", maxLen)
      .build()
    new FieldString(name, (flag & 1) == 1, metadata)
  }

  private def toFieldDateTime(bb: ByteBuffer, name: String, alias: String): Field = {
    val len = bb.get
    val flag = bb.get
    val mask = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .build()
    new FieldDateTime(name, (flag & 1) == 1, metadata)
  }

  private def toFieldOID(bb: ByteBuffer, name: String, alias: String): Field = {
    val len = bb.get
    val flag = bb.get
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .build()
    new FieldOID(name, (flag & 1) == 1, metadata)
  }

  private def toFieldGeom(bb: ByteBuffer, name: String, alias: String, geometryType: Byte, geometryProp: Int): Field = {
    bb.get // unk
    val flag = bb.get // 6 or 7. If lsb is 1, the field can be null.
    val nullAllowed = (flag & 1) == 1

    val crsLen = bb.getShort
    val crsChars = crsLen / 2
    val stringBuilder = new StringBuilder(crsChars)
    0 until crsChars foreach (_ => stringBuilder.append(bb.getChar))
    val crs = stringBuilder.toString

    val zAndM = bb.get
    val (hasZ, hasM) = zAndM match {
      case 7 => (true, true)
      case 5 => (true, false)
      case _ => (false, false)
    }

    // println(s"geometryType=$geometryType zAndM=$zAndM hasZ=$hasZ hasM=$hasM geomProp=$geometryProp")

    val xOrig = bb.getDouble
    val yOrig = bb.getDouble
    val xyScale = bb.getDouble
    val mOrig = if (hasM) bb.getDouble else 0.0
    val mScale = if (hasM) bb.getDouble else 0.0
    val zOrig = if (hasZ) bb.getDouble else 0.0
    val zScale = if (hasZ) bb.getDouble else 0.0
    val xyTolerance = bb.getDouble
    val mTolerance = if (hasM) bb.getDouble else 0.0
    val zTolerance = if (hasZ) bb.getDouble else 0.0
    val xmin = bb.getDouble
    val ymin = bb.getDouble
    val xmax = bb.getDouble
    val ymax = bb.getDouble
    val numes = new ArrayBuffer[Double]()
    var cont = true
    while (cont) {
      val pos = bb.position
      val m1 = bb.get
      val m2 = bb.get
      val m3 = bb.get
      val m4 = bb.get
      val m5 = bb.get
      if (m1 == 0 && m2 > 0 && m3 == 0 && m4 == 0 && m5 == 0) {
        0 until m2 foreach (_ => numes += bb.getDouble)
        cont = false
      }
      else {
        bb.position(pos)
        numes += bb.getDouble
      }
    }

    val metadataBuilder = new MetadataBuilder()
      .putString("alias", alias)
      .putString("crs", crs)
      .putDouble("xmin", xmin)
      .putDouble("ymin", ymin)
      .putDouble("xmax", xmax)
      .putDouble("ymax", ymax)
      .putBoolean("hasZ", hasZ)
      .putBoolean("hasM", hasM)
      .putDouble("xyTolerance", xyTolerance)

    if (hasZ) metadataBuilder.putDouble("zTolerance", zTolerance)
    if (hasM) metadataBuilder.putDouble("mTolerance", mTolerance)

    val metadata = metadataBuilder.build()

    // TODO - more shapes, Z and M
    geometryType match {
      case 1 =>
        geometryProp match {
          case 0x00 => FieldPointType(name, nullAllowed, xOrig, yOrig, xyScale, metadata)
          case 0x40 => FieldPointMType(name, nullAllowed, xOrig, yOrig, mOrig, xyScale, mScale, metadata)
          case 0x80 => FieldPointZType(name, nullAllowed, xOrig, yOrig, zOrig, xyScale, zScale, metadata)
          case _ => FieldPointZMType(name, nullAllowed, xOrig, yOrig, zOrig, mOrig, xyScale, zScale, mScale, metadata)
        }
      case 3 =>
        geometryProp match {
          case 0x00 => FieldPolylineType(name, nullAllowed, xOrig, yOrig, xyScale, metadata)
          case 0x40 => FieldPolylineMType(name, nullAllowed, xOrig, yOrig, mOrig, xyScale, mScale, metadata)
          case _ => throw new RuntimeException("Cannot parse polylines with Z value :-(")
        }
      case 4 | 5 =>
        FieldPolygonType(name, nullAllowed, xOrig, yOrig, xyScale, metadata)
      case _ =>
        new FieldGeomNoop(name, nullAllowed)
    }
  }

  def listTables(path: String, conf: Configuration = new Configuration()) = {
    val index = GDBIndex(path, "a00000001", conf)
    try {
      val table = GDBTable(path, "a00000001", conf)
      try {
        val idxID = table.fields.indexWhere(_.name == "ID")
        val idxName = table.fields.indexWhere(_.name == "Name")
        table.rowIterator(index).map(row => CatRow(row.getInt(idxID), row.getString(idxName))).toArray
      }
      finally {
        table.close()
      }
    } finally {
      index.close()
    }
  }

  def findTable(path: String, tableName: String, conf: Configuration = new Configuration()) = {
    // log.info(s"findTable::$tableName")
    val index = GDBIndex(path, "a00000001", conf)
    try {
      val table = GDBTable(path, "a00000001", conf)
      try {
        table
          .seekIterator(index.iterator())
          .find(row => row("Name") == tableName)
          .map(row => CatRow(row("ID").asInstanceOf[Int], row("Name").asInstanceOf[String]))
      }
      finally {
        table.close()
      }
    } finally {
      index.close()
    }
  }
}