package org.apache.spark.sql.udt

import org.apache.spark.sql.types.SQLUserDefinedType

/**
  */
@SQLUserDefinedType(udt = classOf[PointZUDT])
class PointZType(val x: Double = 0.0, val y: Double = 0.0, val z: Double = 0.0) extends SpatialType {

  def ==(that: PointZType): Boolean = this.x == that.x && this.y == that.y && this.z == that.z

  override def equals(other: Any): Boolean = other match {
    case that: PointZType => this == that
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(x, y, z).foldLeft(0)((a, b) => {
      val bits = java.lang.Double.doubleToLongBits(b)
      31 * a + (bits ^ (bits >>> 32)).toInt
    })
  }

  override def toString = s"PointZType($x,$y,$z)"

}

object PointZType {
  def apply(x: Double, y: Double, z: Double) = new PointZType(x, y, z)

  def unapply(p: PointZType) = Some((p.x, p.y, p.z))
}
