package com.endor.spark

import java.nio.{ByteBuffer, ByteOrder}

package object blockchain {
  implicit class StringOps(val value: String) extends AnyVal {
    def bytes: Array[Byte] = value.replaceFirst("0x", "").grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
    def hexToLong: Long = java.lang.Long.parseLong(value.replaceFirst("0x", ""), 16)
  }

  implicit class ByteArrayOps(val value: Array[Byte]) extends AnyVal {
    def hex: String = value.map("%02x" format _).mkString
    def asLong: Long = {
      val padded = Array.fill[Byte](java.lang.Long.BYTES - value.length){0} ++ value
      ByteBuffer.wrap(padded).order(ByteOrder.BIG_ENDIAN).getLong
    }
    def asInt: Int = {
      val padded = Array.fill[Byte](java.lang.Integer.BYTES - value.length){0} ++ value
      ByteBuffer.wrap(padded).order(ByteOrder.BIG_ENDIAN).getInt
    }
    def asBigInt: BigInt = BigInt(value)
  }

  implicit class Bytes(val value: Long) extends AnyVal {
    def bytes: Array[Byte] = ByteBuffer
      .allocate(java.lang.Long.BYTES)
      .order(ByteOrder.BIG_ENDIAN)
      .putLong(value)
      .array()
      .dropWhile(_ == 0)
  }

  implicit class BigIntOps(val value: BigInt) extends AnyVal {
    def exp(exponent: Int): BigInt = value * BigInt(Math.pow(10, exponent.toDouble).toLong)
  }
}
