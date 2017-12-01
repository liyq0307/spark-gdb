package org.apache.spark.sql.udt

import org.apache.spark.sql.types.SQLUserDefinedType

/**
  * PolygonType
  *
  * @param xyNum each element contains the number of xy pairs to read for a part
  * @param xyArr sequence of xy elements
  */
@SQLUserDefinedType(udt = classOf[PolygonUDT])
class PolygonType(override val xmin: Double,
                  override val ymin: Double,
                  override val xmax: Double,
                  override val ymax: Double,
                  override val xyNum: Array[Int],
                  override val xyArr: Array[Double])
  extends PolyType(xmin, ymin, xmax, ymax, xyNum, xyArr) {

}

object PolygonType {
  def apply(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]) = {
    new PolygonType(xmin, ymin, xmax, ymax, xyNum, xyArr)
  }

  def unapply(p: PolygonType) =
    Some((p.xmin, p.ymin, p.xmax, p.ymax, p.xyNum, p.xyArr))
}
