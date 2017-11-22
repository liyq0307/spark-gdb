package com.esri.gdb

//import com.esri.udt.{PolylineMType, PolylineMUDT}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.udt.{PolylineMType, PolylineMUDT}

/**
  */
object FieldPolylineMType extends Serializable {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            mOrig: Double,
            xyScale: Double,
            mScale: Double,
            metadata: Metadata) = {
    new FieldPolylineMType(name, nullValueAllowed, xOrig, yOrig, mOrig, xyScale, mScale, metadata)
  }
}

class FieldPolylineMType(name: String,
                         nullValueAllowed: Boolean,
                         xOrig: Double,
                         yOrig: Double,
                         mOrig: Double,
                         xyScale: Double,
                         mScale: Double,
                         metadata: Metadata)
  extends FieldPoly3Type[PolylineMType](name, new PolylineMUDT(), nullValueAllowed, xOrig, yOrig, mOrig, xyScale, mScale, metadata) {

  override def createPolyMType(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]) = {
    PolylineMType(xmin, ymin, xmax, ymax, xyNum, xyArr)
  }
}
