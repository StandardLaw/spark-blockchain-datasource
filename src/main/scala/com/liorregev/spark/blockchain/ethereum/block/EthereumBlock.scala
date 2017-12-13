package com.liorregev.spark.blockchain.ethereum.block

import com.liorregev.spark.blockchain._
import org.apache.spark.sql.{Encoder, Encoders}
import org.ethereum.core.{Block => EthereumjBlock, BlockHeader => EthereumjBlockHeader, Transaction => EthereumjTransaction}
import org.ethereum.util.ByteUtil

import scala.collection.JavaConverters._

final case class EthereumBlockHeader(parentHash: Array[Byte], uncleHash: Array[Byte], coinBase: Array[Byte],
                                     stateRoot: Array[Byte], txTrieRoot: Array[Byte], receiptTrieRoot: Array[Byte],
                                     logsBloom: Array[Byte], difficulty: Array[Byte], timestamp: Long, number: Long,
                                     gasLimit: Long, gasUsed: Long, mixHash: Array[Byte], extraData: Array[Byte],
                                     nonce: Array[Byte])

object EthereumBlockHeader {
  def fromEthereumjBlockHeader(blockHeader: EthereumjBlockHeader): EthereumBlockHeader =
    EthereumBlockHeader(blockHeader.getParentHash, blockHeader.getUnclesHash, blockHeader.getCoinbase,
      blockHeader.getStateRoot, blockHeader.getTxTrieRoot, blockHeader.getReceiptsRoot, blockHeader.getLogsBloom,
      blockHeader.getDifficulty, blockHeader.getTimestamp, blockHeader.getNumber,
      blockHeader.getGasLimit.asLong, blockHeader.getGasUsed, blockHeader.getMixHash,
      blockHeader.getExtraData, blockHeader.getNonce)
  implicit val encoder: Encoder[EthereumBlockHeader] = Encoders.product[EthereumBlockHeader]

}

sealed trait EthereumTransaction {
  def nonce: Array[Byte]
  def value: Array[Byte]
  def receiveAddress: Array[Byte]
  def gasPrice: Long
  def gasLimit: Long
  def data: Option[Array[Byte]]
  def sig_v: Byte
  def sig_r: Array[Byte]
  def sig_s: Array[Byte]
  def chainId: Option[java.lang.Integer]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def toEthereumj: EthereumjTransaction = {
    new EthereumjTransaction(nonce, gasPrice.bytes, gasLimit.bytes, receiveAddress, value,
      data.orNull, sig_r, sig_s, sig_v, chainId.orNull)
  }
}

final case class SimpleEthereumTransaction(nonce: Array[Byte], value: Array[Byte], receiveAddress: Array[Byte],
                                     gasPrice: Long, gasLimit: Long, data: Option[Array[Byte]], sig_v: Byte,
                                     sig_r: Array[Byte], sig_s: Array[Byte], chainId: Option[java.lang.Integer])
  extends EthereumTransaction {

  def toEnriched: EnrichedEthereumTransaction = EnrichedEthereumTransaction.fromEthereumjTransaction(toEthereumj)
}

object SimpleEthereumTransaction {
  def fromEthereumjTransaction(transaction: EthereumjTransaction): SimpleEthereumTransaction = {
    val signature = transaction.getSignature
    val value = transaction.getValue
    SimpleEthereumTransaction(transaction.getNonce, if (ByteUtil.isSingleZero(value)) Array() else value,
      transaction.getReceiveAddress, transaction.getGasPrice.asLong, transaction.getGasLimit.asLong,
      Option(transaction.getData), signature.v, signature.r.toByteArray,
      signature.s.toByteArray, Option(transaction.getChainId))
  }
  implicit val encoder: Encoder[SimpleEthereumTransaction] = Encoders.product[SimpleEthereumTransaction]
}

sealed trait EthereumBlock[T <: EthereumTransaction] {
  def ethereumBlockHeader: EthereumBlockHeader
  def ethereumTransactions: Seq[T]
  def uncleHeaders: Seq[EthereumBlockHeader]
}

final case class SimpleEthereumBlock(ethereumBlockHeader: EthereumBlockHeader,
                                     ethereumTransactions: Seq[SimpleEthereumTransaction],
                                     uncleHeaders: Seq[EthereumBlockHeader])
  extends EthereumBlock[SimpleEthereumTransaction] {
  def toEnriched: EnrichedEthereumBlock =
    EnrichedEthereumBlock(ethereumBlockHeader, ethereumTransactions.map(_.toEnriched), uncleHeaders)
}

object SimpleEthereumBlock {
  def fromEthereumjBlock(block: EthereumjBlock): SimpleEthereumBlock = {
    SimpleEthereumBlock(
      EthereumBlockHeader.fromEthereumjBlockHeader(block.getHeader),
      block.getTransactionsList.asScala.map(SimpleEthereumTransaction.fromEthereumjTransaction),
      block.getUncleList.asScala.map(EthereumBlockHeader.fromEthereumjBlockHeader)
    )
  }

  implicit val encoder: Encoder[SimpleEthereumBlock] = Encoders.product[SimpleEthereumBlock]
}

final case class EnrichedEthereumTransaction(nonce: Array[Byte], value: Array[Byte], receiveAddress: Array[Byte],
                                             gasPrice: Long, gasLimit: Long, data: Option[Array[Byte]], sig_v: Byte,
                                             sig_r: Array[Byte], sig_s: Array[Byte], chainId: Option[java.lang.Integer],
                                             sendAddress: Array[Byte], hash: Array[Byte],
                                             contractAddress: Option[Array[Byte]]) extends EthereumTransaction

object EnrichedEthereumTransaction {
  def fromEthereumjTransaction(transaction: EthereumjTransaction): EnrichedEthereumTransaction = {
    val simpleTransaction = SimpleEthereumTransaction.fromEthereumjTransaction(transaction)
    EnrichedEthereumTransaction(simpleTransaction.nonce, simpleTransaction.value, simpleTransaction.receiveAddress,
      simpleTransaction.gasPrice, simpleTransaction.gasLimit, simpleTransaction.data, simpleTransaction.sig_v,
      simpleTransaction.sig_r, simpleTransaction.sig_s, simpleTransaction.chainId,
      transaction.getSender, transaction.getHash, Option(transaction.getContractAddress))
  }
  implicit val encoder: Encoder[EnrichedEthereumTransaction] = Encoders.product[EnrichedEthereumTransaction]
}

final case class EnrichedEthereumBlock(ethereumBlockHeader: EthereumBlockHeader,
                                       ethereumTransactions: Seq[EnrichedEthereumTransaction],
                                       uncleHeaders: Seq[EthereumBlockHeader])
  extends EthereumBlock[EnrichedEthereumTransaction]

object EnrichedEthereumBlock {
  def fromEthereumjBlock(block: EthereumjBlock): EnrichedEthereumBlock = {
    EnrichedEthereumBlock(
      EthereumBlockHeader.fromEthereumjBlockHeader(block.getHeader),
      block.getTransactionsList.asScala.map(EnrichedEthereumTransaction.fromEthereumjTransaction),
      block.getUncleList.asScala.map(EthereumBlockHeader.fromEthereumjBlockHeader)
    )
  }

  implicit val encoder: Encoder[EnrichedEthereumBlock] = Encoders.product[EnrichedEthereumBlock]
}
