package com.liorregev.spark.blockchain.ethereum

import java.math.BigInteger

import org.apache.spark.sql.{Encoder, Encoders}
import org.ethereum.core.CallTransaction

import scala.io.Source

final case class RegisterCall(address: Array[Byte], tla: String, base: BigDecimal, name: String)

object RegisterCall {
  implicit val encoder: Encoder[RegisterCall] = Encoders.product[RegisterCall]
}

object TokenRegistryParser {
  private val abiPath = "/ethereum/abi/tokenRegistry.json"
  private val contract = new CallTransaction.Contract(Source.fromURL(getClass.getResource(abiPath)).mkString)
  def parse(rawData: Array[Byte]): Option[RegisterCall] = {
    val invocation = contract.parseInvocation(rawData)
    invocation.function.name match {
      case "register" =>
        invocation.args match {
          case Array(address: Array[Byte], tla: String, base: BigInteger, name: String) =>
            Option(RegisterCall(address, tla, BigDecimal(base), name))
        }
      case _ => None
    }
  }
}
