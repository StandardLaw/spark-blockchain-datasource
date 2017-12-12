package com.liorregev.spark.blockchain.ethereum.datasource

import com.liorregev.spark.blockchain.ethereum.RegisterCall
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
      case (x, y) =>
        x == y
    }

  implicit val blockHeaderEquality: Equality[EthereumBlockHeader] = (a: EthereumBlockHeader, b: Any) =>
    (a, b) match {
      case (headerA: EthereumBlockHeader, headerB: EthereumBlockHeader) => productEquality(headerA, headerB)
      case _ => false
    }

  implicit val transactionEquality: Equality[SimpleEthereumTransaction] = (a: SimpleEthereumTransaction, b: Any) =>
    (a, b) match {
      case (transactionA: SimpleEthereumTransaction, transactionB: SimpleEthereumTransaction) =>
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

  implicit val blockEquality: Equality[SimpleEthereumBlock] =
    (a: SimpleEthereumBlock, b: Any) =>
      (a, b) match {
        case (blockA: SimpleEthereumBlock, blockB: SimpleEthereumBlock) =>
          val headerEqual = blockHeaderEquality.areEqual(blockA.ethereumBlockHeader, blockB.ethereumBlockHeader)
          val transactionsEqual = blockA.ethereumTransactions
            .zip(blockB.ethereumTransactions)
            .forall(x => transactionEquality.areEqual(x._1, x._2))
          val uncleEqual = blockA.uncleHeaders
            .zip(blockB.uncleHeaders)
            .forall(x => blockHeaderEquality.areEqual(x._1, x._2))

          headerEqual && transactionsEqual && uncleEqual
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

  implicit val registerCallEquality: Equality[RegisterCall] = (callA: RegisterCall, b: Any) => b match {
    case (callB: RegisterCall) =>
      productEquality(callA, callB)
    case _ => false
  }
}
