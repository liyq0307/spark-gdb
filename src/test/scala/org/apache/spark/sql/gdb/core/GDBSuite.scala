package org.apache.spark.sql.gdb.core

import org.apache.spark.sql.gdb.udf.{PointType, PolygonType, PolylineMType, PolylineType}
import org.apache.spark.sql.{DataFrame, SQLContext, gdb}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  */
class GDBSuite extends FunSuite with BeforeAndAfterAll {
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val config = new SparkConf()
      .setMaster("local")
      .setAppName("GDBSuite")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")
    sc = new SparkContext(config)
    sqlContext = new SQLContext(sc)
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  private val gdbPath = "E:/Data/GDB/raster.gdb"

  test("123") {
    val df = sqlContext.gdbFile(gdbPath, "AMD_test20160811_OVR", 8)
    df.show()
  }

  test("Points") {
    doPoints(sqlContext.gdbFile(gdbPath, "Points", 2))
  }

  def doPoints(dataFrame: DataFrame): Unit = {
    dataFrame.createOrReplaceTempView("Points")
    val ss = sqlContext.sql("select shape from Points where OBJECTID = 2").collect()(0)
    ss.get(0).asInstanceOf[PointType]
    dataFrame.agg(("OBJECTID", "max"), ("OBJECTID", "min")).show()
    dataFrame.show()
    val xyTolerance = dataFrame.schema("Shape").metadata.getDouble("xyTolerance")

    val results = dataFrame.select("Shape", "X", "Y", "RID", "OBJECTID")
    results.collect.foreach(row => {
      val point = row.getAs[PointType](0)
      val x = row.getDouble(1)
      val y = row.getDouble(2)
      val rid = row.getInt(3)
      val oid = row.getInt(4)
      assert((point.x - x).abs <= xyTolerance)
      assert((point.y - y).abs <= xyTolerance)
      assert(rid === oid)
    })
  }

  test("Lines") {
    doLines(sqlContext.gdbFile(gdbPath, "Lines", 2))
    //doLines(sqlContext.gdbFile("F:\\Data\\GDB\\OsmData.gdb", "highway", 2))
  }

  def doLines(dataFrame: DataFrame): Unit = {
    val xyTolerance = dataFrame.schema("Shape").metadata.getDouble("xyTolerance")

    val results = dataFrame.select("Shape",
      "X1", "Y1",
      "X2", "Y2",
      "X3", "Y3",
      "RID",
      "OBJECTID")
    results.collect.foreach(row => {
      val polyline = row.getAs[PolylineType](0)
      val x1 = row.getDouble(1)
      val y1 = row.getDouble(2)
      val x2 = row.getDouble(3)
      val y2 = row.getDouble(4)
      val x3 = row.getDouble(5)
      val y3 = row.getDouble(6)
      val rid = row.getInt(7)
      val oid = row.getInt(8)

      assert(polyline.xyNum.length === 1)
      assert(polyline.xyNum(0) === 3)

      assert(polyline.xyArr.length === 6)

      assert((polyline.xyArr(0) - x1).abs <= xyTolerance)
      assert((polyline.xyArr(1) - y1).abs <= xyTolerance)

      assert((polyline.xyArr(2) - x2).abs <= xyTolerance)
      assert((polyline.xyArr(3) - y2).abs <= xyTolerance)

      assert((polyline.xyArr(4) - x3).abs <= xyTolerance)
      assert((polyline.xyArr(5) - y3).abs <= xyTolerance)

      assert(rid === oid)
    })
  }

  test("MLines") {
    doMLines(sqlContext.gdbFile(gdbPath, "MLines", 2))
  }

  def doMLines(dataFrame: DataFrame): Unit = {
    dataFrame.show()
    val metadata = dataFrame.schema("Shape").metadata
    val xyTolerance = metadata.getDouble("xyTolerance")
    val mTolerance = metadata.getDouble("mTolerance")

    val results = dataFrame.select("Shape",
      "X1", "Y1", "M1",
      "X2", "Y2", "M2",
      "X3", "Y3", "M3",
      "RID",
      "OBJECTID")
    results.collect.foreach(row => {
      val polyline = row.getAs[PolylineMType](0)
      val x1 = row.getDouble(1)
      val y1 = row.getDouble(2)
      val m1 = row.getDouble(3)
      val x2 = row.getDouble(4)
      val y2 = row.getDouble(5)
      val m2 = row.getDouble(6)
      val x3 = row.getDouble(7)
      val y3 = row.getDouble(8)
      val m3 = row.getDouble(9)
      val rid = row.getInt(10)
      val oid = row.getInt(11)

      assert(rid === oid)

      assert(polyline.xyNum.length === 1)
      assert(polyline.xyNum(0) === 3)

      assert(polyline.xyArr.length === 9)

      assert((polyline.xyArr(0) - x1).abs <= xyTolerance)
      assert((polyline.xyArr(1) - y1).abs <= xyTolerance)
      assert((polyline.xyArr(2) - m1).abs <= mTolerance)

      assert((polyline.xyArr(3) - x2).abs <= xyTolerance)
      assert((polyline.xyArr(4) - y2).abs <= xyTolerance)
      assert((polyline.xyArr(5) - m2).abs <= mTolerance)

      assert((polyline.xyArr(6) - x3).abs <= xyTolerance)
      assert((polyline.xyArr(7) - y3).abs <= xyTolerance)
      assert((polyline.xyArr(8) - m3).abs <= mTolerance)
    })
  }

  test("Polygons") {
    doPolygons(sqlContext.gdbFile(gdbPath, "Polygons", 2))
    //doPolygons(sqlContext.gdbFile("F:\\Data\\GDB\\OsmData.gdb", "landuse", 2))
  }

  def doPolygons(dataFrame: DataFrame): Unit = {
    dataFrame.filter("OBJECTID >=1 AND OBJECTID <= 2").show()
    val xyTolerance = dataFrame.schema("Shape").metadata.getDouble("xyTolerance")

    val results = dataFrame.select("Shape",
      "X1", "Y1",
      "X2", "Y2",
      "RID",
      "OBJECTID")
    results.collect.foreach(row => {
      val polygon = row.getAs[PolygonType](0)
      val x1 = row.getDouble(1)
      val y1 = row.getDouble(2)
      val x2 = row.getDouble(3)
      val y2 = row.getDouble(4)
      val rid = row.getInt(5)
      val oid = row.getInt(6)

      assert((polygon.xmin - x1).abs < xyTolerance)
      assert((polygon.ymin - y1).abs < xyTolerance)

      assert((polygon.xmax - x2).abs < xyTolerance)
      assert((polygon.ymax - y2).abs < xyTolerance)

      assert(rid === oid)
    })
  }

  test("DDL test") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE points
         |USING ${gdb.sparkGDB}
         |OPTIONS (path "$gdbPath", name "Points", numPartitions "1")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT * FROM points").collect().length === 20)
  }

  test("Field names, aliases and values") {
    val dataframe = sqlContext.gdbFile(gdbPath, "Types", 2)
    val schema = dataframe.schema

    val fieldShape = schema("Shape")
    assert(fieldShape.name === "Shape")
    assert(fieldShape.metadata.getString("alias") === "Shape")

    val fieldAText = schema("A_TEXT")
    assert(fieldAText.name === "A_TEXT")
    assert(fieldAText.metadata.getString("alias") === "A Text")
    assert(fieldAText.metadata.getLong("maxLength") === 32)

    val fieldAFloat = schema("A_FLOAT")
    assert(fieldAFloat.name === "A_FLOAT")
    assert(fieldAFloat.metadata.getString("alias") === "A Float")

    val fieldADouble = schema("A_DOUBLE")
    assert(fieldADouble.name === "A_DOUBLE")
    assert(fieldADouble.metadata.getString("alias") === "A Double")

    val fieldAShort = schema("A_SHORT")
    assert(fieldAShort.name === "A_SHORT")
    assert(fieldAShort.metadata.getString("alias") === "A Short")

    val fieldALong = schema("A_LONG")
    assert(fieldALong.name === "A_LONG")
    assert(fieldALong.metadata.getString("alias") === "A Long")

    val fieldADate = schema("A_DATE")
    assert(fieldADate.name === "A_DATE")
    assert(fieldADate.metadata.getString("alias") === "A Date")

    val fieldAGuid = schema("A_GUID")
    assert(fieldAGuid.name === "A_GUID")
    assert(fieldAGuid.metadata.getString("alias") === "A GUID")

    val row = dataframe
      .select("Shape", "A_TEXT", "A_FLOAT", "A_DOUBLE", "A_SHORT", "A_LONG", "A_DATE", "A_GUID")
      .collect()
      .head

    val point = row.getAs[PointType](0)
    assert((point.x - 33.8869).abs < 0.00001)
    assert((point.y - 35.5131).abs < 0.00001)

    assert(row.getString(1) === "Beirut")

    assert((row.getFloat(2) - 33.8869).abs < 0.00001)
    assert((row.getDouble(3) - 35.5131).abs < 0.00001)

    assert(row.getShort(4) === 33)
    assert(row.getInt(5) === 35)

    val timestamp = row.getTimestamp(6)
    val datetime = new DateTime(timestamp.getTime, DateTimeZone.UTC)
    // 2016, 01, 01, 07, 24, 32
    assert(datetime.getYear === 2016)
    assert(datetime.getMonthOfYear === 1)
    assert(datetime.getDayOfMonth === 1)
    assert(datetime.getHourOfDay === 7)
    assert(datetime.getMinuteOfHour === 24)
    assert(datetime.getSecondOfMinute === 32)

    assert(row.getString(7) === "{2AA7D58D-2BF4-4943-83A8-457B70DB1871}")
  }
}
