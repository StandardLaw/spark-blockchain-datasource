package com.liorregev.spark.blockchain.ethereum

import com.liorregev.spark.blockchain._
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.{Encoder, Encoders}

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
final case class RegisterCall(address: Array[Byte], tla: String, base: BigDecimal, name: String)

object RegisterCall {
  implicit val encoder: Encoder[RegisterCall] = Encoders.product[RegisterCall]
}

object TokenRegistryParser {
  def parseRegisterCalls(rawData: Array[Byte]): Option[RegisterCall] = {
    val registerMethodId = "66b42dcb"
    val (methodID, data) = rawData.splitAt(4)
    methodID.hex match {
      case `registerMethodId` =>
        val address = data.slice(12, 32)
        val tlaPos = data.slice(32, 64).asBigInt.toInt
        val base = data.slice(64, 96).asBigInt
        val namePos = data.slice(96, 128).asBigInt.toInt
        val tlaSize = data.slice(tlaPos, tlaPos + 32).asBigInt.toInt
        val tla = new String(data.slice(tlaPos + 32, tlaPos + 32 + tlaSize), StandardCharsets.UTF_8)
        val nameSize = data.slice(namePos, namePos + 32).asBigInt.toInt
        val name = new String(data.slice(namePos + 32, namePos + 32 + nameSize), StandardCharsets.UTF_8)
        Option(RegisterCall(address, tla, BigDecimal(base), name))
      case _ =>
        None
    }
  }
}
