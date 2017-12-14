package com.liorregev.spark.blockchain.bitcoin.block

import com.liorregev.spark.blockchain.bitcoin.Hash
import java.sql.Timestamp

import org.apache.spark.sql.{Encoder, Encoders}
import org.bitcoinj.core.{Block, TransactionInput, TransactionOutput, Transaction => BitcoinjTransaction}

final case class Input(previousTransactionHash: Hash, previousTransactionOutputIndex: Long,
                       scriptLength: BigDecimal, script: Array[Byte], sequenceNo: Array[Byte])
object Input {
  implicit val encoder: Encoder[Input] = Encoders.product[Input]
  def fromBitcoinjInput(input: TransactionInput): Input = ???
}

final case class Output(value: BigDecimal, scriptLength: BigDecimal, script: Array[Byte])
object Output {
  implicit val encoder: Encoder[Output] = Encoders.product[Output]
  def fromBitcoinjOutput(input: TransactionOutput): Output = ???
}

final case class Transaction(version: Int, numInputs: BigDecimal, inputs: Seq[Input], numOutputs: BigDecimal,
                             outputs: Seq[Output], lockTime: Array[Byte])
object Transaction {
  implicit val encoder: Encoder[Transaction] = Encoders.product[Transaction]
  def fromBitcoinjTransaction(transaction: BitcoinjTransaction): Transaction = ???
}

final case class BlockHeader(version: Int, previousBlockHash: Hash, merkleRootHash: Hash,
                             timestamp: Timestamp, bits: Long, nonce: Long)
object BlockHeader {
  implicit val encoder: Encoder[BlockHeader] = Encoders.product[BlockHeader]
  def fromBitcoinjBlock(block: Block): BlockHeader = ???
}

final case class BitcoinBlock(magicNumber: String, blockSize: Long, header: BlockHeader,
                              numTransactions: BigDecimal, transactions: Seq[Transaction])
object BitcoinBlock {
  implicit val encoder: Encoder[BitcoinBlock] = Encoders.product[BitcoinBlock]

  def fromBitcoinjBlock(block: Block): BitcoinBlock = ???
}
