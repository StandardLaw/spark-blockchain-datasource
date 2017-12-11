package com.liorregev.spark.blockchain.ethereum.datasource

import com.liorregev.spark.blockchain.ethereum.model._
import org.scalactic.Equality


@SuppressWarnings(Array("org.wartremover.warts.Serializable"))
trait EthereumTestUtils {
  def productEquality[T <: Product](left: T, right: T): Boolean =
    left.productIterator.zip(right.productIterator).forall {
      case (x: Option[_], y: Option[_]) =>
        productEquality(x, y)
      case (x: Array[_], y: Array[_]) =>
        x sameElements y
      case (x: Long, y: Long) =>
        x == y
      case (x: Int, y: Int) =>
        x == y
      case (x: Byte, y: Byte) =>
        x == y
    }

  implicit val blockHeaderEquality: Equality[EthereumBlockHeader] = (a: EthereumBlockHeader, b: Any) =>
    (a, b) match {
      case (headerA: EthereumBlockHeader, headerB: EthereumBlockHeader) => productEquality(headerA, headerB)
      case _ => false
    }

  implicit val transactionEquality: Equality[EthereumTransaction] = (a: EthereumTransaction, b: Any) =>
    (a, b) match {
      case (transactionA: EthereumTransaction, transactionB: EthereumTransaction) =>
        productEquality(transactionA, transactionB)
      case _ => false
    }

  implicit val enrichedTransactionEquality: Equality[EnrichedEthereumTransaction] =
    (a: EnrichedEthereumTransaction, b: Any) =>
      (a, b) match {
        case (transactionA: EnrichedEthereumTransaction, transactionB: EnrichedEthereumTransaction) =>
          productEquality(transactionA, transactionB)
        case _ => false
      }

  implicit val blockEquality: Equality[EthereumBlock] =
    (a: EthereumBlock, b: Any) =>
      (a, b) match {
        case (blockA: EthereumBlock, blockB: EthereumBlock) =>
          blockHeaderEquality.areEqual(blockA.ethereumBlockHeader, blockB.ethereumBlockHeader) &&
            blockA.ethereumTransactions.zip(blockB.ethereumTransactions).forall(x => transactionEquality.areEqual(x._1, x._2)) &&
            blockA.uncleHeaders.zip(blockB.uncleHeaders).forall(x => blockHeaderEquality.areEqual(x._1, x._2))
        case _ => false
      }

  implicit val enrichedBlockEquality: Equality[EnrichedEthereumBlock] =
    (a: EnrichedEthereumBlock, b: Any) =>
      (a, b) match {
        case (blockA: EnrichedEthereumBlock, blockB: EnrichedEthereumBlock) =>
          val headersEqual = blockHeaderEquality.areEqual(blockA.ethereumBlockHeader, blockB.ethereumBlockHeader)
          val transactionsEqual = blockA.ethereumTransactions
            .zip(blockB.ethereumTransactions)
            .forall(x => enrichedTransactionEquality.areEqual(x._1, x._2))
          val unclesEqual = blockA.uncleHeaders
            .zip(blockB.uncleHeaders)
            .forall(x => blockHeaderEquality.areEqual(x._1, x._2))
          headersEqual && transactionsEqual && unclesEqual
        case _ => false
      }
}
