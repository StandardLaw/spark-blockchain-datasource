package com.liorregev

import java.nio.{ByteBuffer, ByteOrder}

package object blockchain {
  private[blockchain] implicit class Unhex(val value: String) extends AnyVal {
    def bytes: Array[Byte] = value.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
  }

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  private[blockchain] implicit class ByteArrayOps(val value: Array[Byte]) extends AnyVal {
    def hex: String = value.map("%02x" format _).mkString
    def asLong: Long = value.zipWithIndex.foldLeft(0L) {
      case (sum, (currentByte, idx)) => sum + (currentByte & 0xFF) * Math.pow(256, idx.toDouble).toLong
    }
    def asInt: Int = asLong.toInt
  }

  private[blockchain] implicit class Bytes(val value: Long) extends AnyVal {
    def bytes: Array[Byte] = ByteBuffer
      .allocate(java.lang.Long.BYTES)
      .order(ByteOrder.BIG_ENDIAN)
      .putLong(value)
      .array()
      .dropWhile(_ == 0)
  }

  private[blockchain] implicit class BigIntOps(val value: BigInt) extends AnyVal {
    def exp(exponent: Int): BigInt = value * BigInt(Math.pow(10, exponent.toDouble).toLong)
  }
}
