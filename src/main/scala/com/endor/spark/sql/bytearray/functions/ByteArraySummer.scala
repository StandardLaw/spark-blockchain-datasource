package com.endor.spark.sql.bytearray.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

class ByteArraySummer extends UserDefinedAggregateFunction {
  private def padArray(input: Array[Byte]): Array[Byte] =
    Array.fill[Byte](Math.max(java.lang.Long.BYTES - input.length, 1)){0} ++ input

  override def inputSchema: StructType = new StructType().add("binaryInput", DataTypes.BinaryType)
  override def bufferSchema: StructType = new StructType().add("binaryInput", DataTypes.BinaryType)
  override def dataType: DataType = DataTypes.BinaryType
  override def deterministic: Boolean = true
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, Array.fill[Byte](1){0})
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val newValue = input.getAs[Array[Byte]](0)
    if (newValue.length > 0) {
      val bufferValue = buffer.getAs[Array[Byte]](0)
      buffer.update(0, (BigInt(padArray(newValue)) + BigInt(padArray(bufferValue))).toByteArray)
    }
  }
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val mergedValue = Seq[Row](buffer1, buffer2).map(_.getAs[Array[Byte]](0)).map(padArray).map(BigInt.apply).sum
    buffer1.update(0, mergedValue.toByteArray)
  }
  override def evaluate(buffer: Row): Any = buffer.getAs[Array[Byte]](0)
}
