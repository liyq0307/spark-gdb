package org.apache.spark.sql.gdb

import org.apache.spark.sql.gdb.core.{GDBIndex, GDBTable}

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer

/**
  * Created by liyq on 2018/11/15.
  */
object GDBUtils {
  /**
    * 常量定义
    */
  private val GDB_Items = "GDB_Items"
  private val Definition = "Definition"
  private val Name = "Name"
  private val ClsID = "CLSID"
  private val DEFeatureClassInfo = "DEFeatureClassInfo"
  private val DETableInfo = "DETableInfo"
  private val GDB_FeatureClasses = "GDB_FeatureClasses"
  private val GDB_ObjectClasses = "GDB_ObjectClasses"
  private val ObjectClassID = "ObjectClassID"
  private val GeometryType = "GeometryType"
  private val ShapeField = "ShapeField"
  private val KeyWord = HashSet("ND_50_DirtyAreas", "ND_50_DirtyObjects")

  /**
    * 获取GDB所包含的数据集名称
    *
    * @param gdbPath gdb文件了路径
    * @return
    */
  def getTableNames(gdbPath: String): Array[String] = {
    val names = new ArrayBuffer[String]()
    val catTab = GDBTable.findTable(gdbPath, GDB_Items).orNull
    if (null != catTab) {
      val index = GDBIndex(gdbPath, catTab.hexName)
      try {
        val table = GDBTable(gdbPath, catTab.hexName)
        var nDefinition = -1
        var nameIndex = -1

        try {
          for (i <- table.fields.indices) {
            if (table.fields(i).name == Definition) {
              nDefinition = i
            }

            if (table.fields(i).name == Name) {
              nameIndex = i
            }
          }

          val tableIte = table.rowIterator(index, 0, index.numRows)
          while (tableIte.hasNext()) {
            val thisTable = tableIte.next()
            val definition = thisTable.get(nDefinition)
            val name = thisTable.get(nameIndex)
            if (null != definition && null != name) {
              if (definition.asInstanceOf[String].contains(DEFeatureClassInfo) ||
                definition.asInstanceOf[String].contains(DETableInfo)) {
                names += name.asInstanceOf[String]
              }
            }
          }
        } finally {
          table.close()
        }
      } finally {
        index.close()
      }
    } else {
      val objCatTab = GDBTable.findTable(gdbPath, GDB_ObjectClasses).orNull
      if (null != objCatTab) {
        val aosName = new ArrayBuffer[String]()
        val objIndex = GDBIndex(gdbPath, objCatTab.hexName)

        try {
          val objTable = GDBTable(gdbPath, objCatTab.hexName)
          try {
            var nameIndex = -1
            var iCLSID = -1

            for (i <- objTable.fields.indices) {
              if (objTable.fields(i).name == ClsID) {
                iCLSID = i
              }

              if (objTable.fields(i).name == Name) {
                nameIndex = i
              }
            }

            val objTableIte = objTable.rowIterator(objIndex, 0, objIndex.numRows)
            while (objTableIte.hasNext()) {
              val thisTable = objTableIte.next()
              val clsId = thisTable.get(iCLSID)
              val name = thisTable.get(nameIndex)
              if (null != clsId && null != name) {
                aosName += name.asInstanceOf[String]
              }
            }
          } finally {
            objTable.close()
          }
        }
        finally {
          objIndex.close()
        }

        val feaCatTab = GDBTable.findTable(gdbPath, GDB_FeatureClasses).orNull
        if (null != feaCatTab) {
          val feaIndex = GDBIndex(gdbPath, feaCatTab.hexName)
          try {
            val feaTable = GDBTable(gdbPath, feaCatTab.hexName)

            try {
              var objID = -1
              var geoType = -1
              var shapeIndex = -1

              for (i <- feaTable.fields.indices) {
                if (feaTable.fields(i).name == ObjectClassID) {
                  objID = i
                }

                if (feaTable.fields(i).name == GeometryType) {
                  geoType = i
                }

                if (feaTable.fields(i).name == ShapeField) {
                  shapeIndex = i
                }
              }

              val feaTableIte = feaTable.rowIterator(objIndex, 0, objIndex.numRows)
              while (feaTableIte.hasNext()) {
                val thisTable = feaTableIte.next()
                val obj = thisTable.get(objID)
                val geo = thisTable.get(geoType)
                val shape = thisTable.get(shapeIndex)

                if (null != obj && null != geo && null != shape) {
                  val idx = obj.asInstanceOf[Int]
                  if (idx > 0 && idx <= aosName.length && aosName.nonEmpty) {
                    names += aosName(idx - 1)
                  }
                }
              }
            } finally {
              feaTable.close()
            }
          } finally {
            feaIndex.close()
          }
        }
      }
    }

    names.filter(f => !KeyWord.contains(f)).toArray
  }

  def getIDName(path: String, name: String): String = {
    var objName: String = null
    val catTab = GDBTable.findTable(path, name).orNull
    if (null != catTab) {
      val table = GDBTable(path, catTab.hexName)
      try {
        objName = table.getIDName
      } finally {
        table.close()
      }
    }

    objName
  }
}
