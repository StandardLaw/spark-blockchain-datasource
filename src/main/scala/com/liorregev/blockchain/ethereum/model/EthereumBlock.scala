package com.liorregev.blockchain.ethereum.model

import com.liorregev.blockchain._
import org.apache.spark.sql.{Encoder, Encoders}
import org.ethereum.core.{
  Block => EthereumjBlock,
  BlockHeader => EthereumjBlockHeader,
  Transaction => EthereumjTransaction
}
import org.ethereum.util.ByteUtil

import scala.collection.JavaConverters._

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
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
      blockHeader.getGasLimit.reverse.asLong, blockHeader.getGasUsed, blockHeader.getMixHash,
      blockHeader.getExtraData, blockHeader.getNonce)
  implicit val encoder: Encoder[EthereumBlockHeader] = Encoders.product[EthereumBlockHeader]

}

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals", "org.wartremover.warts.Null"))
final case class EthereumTransaction(nonce: Array[Byte], value: Array[Byte], receiveAddress: Array[Byte],
                                     gasPrice: Long, gasLimit: Long, data: Option[Array[Byte]], sig_v: Byte,
                                     sig_r: Array[Byte], sig_s: Array[Byte], chainId: Option[java.lang.Integer]) {
  def toEnriched: EnrichedEthereumTransaction = {
    val ethereumjTransaction = toEthereumj
    EnrichedEthereumTransaction.fromEthereumjTransaction(ethereumjTransaction)
  }

  def toEthereumj: EthereumjTransaction = {
    new EthereumjTransaction(nonce, gasPrice.bytes, gasLimit.bytes, receiveAddress, value,
      data.orNull, sig_r, sig_s, sig_v, chainId.orNull)
  }
}

object EthereumTransaction {
  def fromEthereumjTransaction(transaction: EthereumjTransaction): EthereumTransaction = {
    val signature = transaction.getSignature
    val value = transaction.getValue
    EthereumTransaction(transaction.getNonce, if (ByteUtil.isSingleZero(value)) Array() else value,
      transaction.getReceiveAddress, transaction.getGasPrice.reverse.asLong, transaction.getGasLimit.reverse.asLong,
      Option(transaction.getData), signature.v, signature.r.toByteArray,
      signature.s.toByteArray, Option(transaction.getChainId))
  }
  implicit val encoder: Encoder[EthereumTransaction] = Encoders.product[EthereumTransaction]
}

final case class EthereumBlock(ethereumBlockHeader: EthereumBlockHeader,
                               ethereumTransactions: Seq[EthereumTransaction],
                               uncleHeaders: Seq[EthereumBlockHeader]) {
  def toEnriched: EnrichedEthereumBlock =
    EnrichedEthereumBlock(ethereumBlockHeader, ethereumTransactions.map(_.toEnriched), uncleHeaders)
}

object EthereumBlock {
  def fromEthereumjBlock(block: EthereumjBlock): EthereumBlock = {
    EthereumBlock(
      EthereumBlockHeader.fromEthereumjBlockHeader(block.getHeader),
      block.getTransactionsList.asScala.map(EthereumTransaction.fromEthereumjTransaction),
      block.getUncleList.asScala.map(EthereumBlockHeader.fromEthereumjBlockHeader)
    )
  }

  implicit val encoder: Encoder[EthereumBlock] = Encoders.product[EthereumBlock]
}

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
final case class EnrichedEthereumTransaction(nonce: Array[Byte], value: Array[Byte], receiveAddress: Array[Byte],
                                             gasPrice: Long, gasLimit: Long, data: Option[Array[Byte]], sig_v: Byte,
                                             sig_r: Array[Byte], sig_s: Array[Byte], chainId: Option[java.lang.Integer],
                                             sendAddress: Array[Byte], hash: Array[Byte],
                                             contractAddress: Option[Array[Byte]])

object EnrichedEthereumTransaction {
  def fromEthereumjTransaction(transaction: EthereumjTransaction): EnrichedEthereumTransaction = {
    val simpleTransaction = EthereumTransaction.fromEthereumjTransaction(transaction)
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
