package com.esri.gdb

import org.apache.spark.sql.types.{DataType, Metadata}

/**
  */
@deprecated("use FieldBytes instead", "0.4")
abstract class FieldGeom(name: String,
                         dataType: DataType,
                         nullValueAllowed: Boolean,
                         xorig: Double,
                         yorig: Double,
                         xyscale: Double,
                         metadata: Metadata)
  extends FieldBytes(name, dataType, nullValueAllowed, metadata)