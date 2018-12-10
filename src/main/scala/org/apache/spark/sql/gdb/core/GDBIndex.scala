package org.apache.spark.sql.gdb.core

import java.io.File
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.internal.Logging

object GDBIndex {
  def apply(path: String, name: String, conf: Configuration = new Configuration()) = {
    val filename = StringBuilder.newBuilder.append(path).append(File.separator).append(name).append(".gdbtablx").toString()
    val hdfsPath = new Path(filename)
    val dataInput = hdfsPath.getFileSystem(conf).open(hdfsPath)

    val bytes = new Array[Byte](16)
    dataInput.readFully(bytes)
    val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    val signature = byteBuffer.getInt
    val n1024Blocks = byteBuffer.getInt
    val numRows = byteBuffer.getInt
    val indexSize = byteBuffer.getInt

    var tableXBlockMap: Array[Byte] = null
    if (n1024Blocks != 0) {
      dataInput.seek(indexSize * 1024 * n1024Blocks + 16)
      dataInput.readFully(bytes)
      val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      val nMagic = byteBuffer.getInt
      val nBitsForBlockMap = byteBuffer.getInt
      assert(nBitsForBlockMap < 2147483647 / 1024)
      val n1024BlocksBis = byteBuffer.getInt
      assert(n1024BlocksBis == n1024Blocks)
      if (nMagic != 0) {
        assert(numRows < nBitsForBlockMap * 1024)
        val nSizeInBytes = (nBitsForBlockMap +7)/8
        tableXBlockMap = new Array[Byte](nSizeInBytes)
        dataInput.readFully(tableXBlockMap)
      }
    }

    new GDBIndex(dataInput, numRows, indexSize, tableXBlockMap)
  }

  private[gdb] def TestBit(blockMap: Array[Byte], bit: Int): Int = {
    val offset = bit / 8
    val b: Int = blockMap(offset).toInt
    b & (1 << (bit % 8))
  }
}

private[gdb] class GDBIndex(dataInput: FSDataInputStream,
                            val numRows: Int,
                            indexSize: Int,
                            blockMap: Array[Byte]
                           ) extends Logging with AutoCloseable with Serializable {
  def readSeekForRowNum(rowNum: Int): Int = {
    val bytes = new Array[Byte](indexSize)
    dataInput.seek(16 + rowNum * indexSize)
    dataInput.readFully(bytes)
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  def iterator(startAtRow: Int = 0,
               numRowsToRead: Int = -1,
               idx: Int = 0,
               bValue: Int = 0): Iterator[IndexInfo] = {
    if (null == blockMap) {
      dataInput.seek(16 + startAtRow * indexSize)
    }
    val maxRows = if (numRowsToRead == -1) numRows else numRowsToRead
    // log.info(s"iterator::startAtRow=$startAtRow maxRows=$maxRows")
    new GDBIndexIterator(dataInput, startAtRow, maxRows, indexSize, blockMap).withFilter(_.isSeekable)
  }

  def close() {
    dataInput.close()
  }
}

private[gdb] class GDBIndexIterator(dataInput: FSDataInputStream,
                                    startID: Int,
                                    maxRows: Int,
                                    indexSize: Int,
                                    blockMap: Array[Byte]
                                   ) extends Iterator[IndexInfo] with Logging with Serializable {

  private val indexInfo = IndexInfo(0, 0)
  private val bytes = new Array[Byte](indexSize)
  private val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

  private var objectID = startID
  private var nextRow = 0
  private var iBlockIdx = 0
  private var iBlockValue = 0

  def hasNext() = nextRow < maxRows

  def next() = {
    // log.info(s"next::nextRow=$nextRow maxRows=$maxRows")
    nextRow += 1

    objectID += 1
    indexInfo.objectID = objectID

    var bSeek = true

    if (null != blockMap) {
      var blocksBefore = 0
      val iBlock = (objectID -1) / 1024
      if (GDBIndex.TestBit(blockMap, iBlock) == 0) {
        bSeek = false
      } else {
        if (iBlock >= iBlockIdx) {
          blocksBefore = iBlockValue
          for (i <- iBlockIdx until iBlock) {
            if (GDBIndex.TestBit(blockMap, i) != 0) {
              blocksBefore += 1
            }
          }
        } else {
          for (i <- 0 until iBlock) {
            if (GDBIndex.TestBit(blockMap, i) != 0) {
              blocksBefore += 1
            }
          }
        }

        iBlockIdx = iBlock
        iBlockValue = blocksBefore
//        print("row =" +  (objectID -1) + " iBlockIdx = " + iBlockIdx + " iBlockValue = "+ iBlockValue + "\n")
        val iCorrectedRow = blocksBefore * 1024 + ((objectID -1) % 1024)
        dataInput.seek(16 + indexSize * iCorrectedRow)
      }
    }

    if (bSeek) {
      byteBuffer.clear
      dataInput.readFully(bytes)

      indexInfo.seek = if (indexSize == 4) {
        byteBuffer.getUInt()
      } else if (indexSize == 5) {
        val byte4: Long = byteBuffer.get(4).toLong
        byteBuffer.getUInt() | (byte4 << 32)
      } else {
        val byte4: Long = byteBuffer.get(4).toLong
        val byte5: Long = byteBuffer.get(5).toLong
        byteBuffer.getUInt() | (byte4 << 32) | (byte5 << 40)
      }
    } else {
      indexInfo.seek = 0L
    }

    indexInfo
  }
}
