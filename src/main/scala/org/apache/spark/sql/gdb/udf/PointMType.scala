package org.apache.spark.sql.gdb.udf

import org.apache.spark.sql.types.SQLUserDefinedType

/**
  */
@SQLUserDefinedType(udt = classOf[PointMUDT])
class PointMType(val x: Double = 0.0, val y: Double = 0.0, val m: Double = 0.0) extends SpatialType {

  def ==(that: PointMType): Boolean = this.x == that.x && this.y == that.y && this.m == that.m

  override def equals(other: Any): Boolean = other match {
    case that: PointMType => this == that
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(x, y, m).foldLeft(0)((a, b) => {
      val bits = java.lang.Double.doubleToLongBits(b)
      31 * a + (bits ^ (bits >>> 32)).toInt
    })
  }

  override def toString = s"PointMType($x,$y,$m)"

}

object PointMType {
  def apply(x: Double, y: Double, m: Double) = new PointMType(x, y, m)

  def unapply(p: PointMType) = Some((p.x, p.y, p.m))
}
