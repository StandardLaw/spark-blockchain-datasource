package com.endor.spark.sql.bytearray

package object functions {
  def sum(singedValues: Boolean): ByteArraySummer = new ByteArraySummer(singedValues)
}
