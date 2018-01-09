package com.endor.spark.blockchain.bitcoin

import com.endor.spark.blockchain._
import com.endor.spark.blockchain.bitcoin.transactions.{Transaction => SourceTransaction}
import org.apache.spark.sql.Dataset

package object enriched {
  private def createJoinExpr(transaction: Hash, index: Long): String = {
    s"${transaction.hex}-$index"
  }

  private def transactionsToRawOutputs(transactions: Dataset[SourceTransaction]): Dataset[RawOutput] = {
    transactions
      .flatMap { (transaction: SourceTransaction) =>
        transaction.outputs
          .map(output => RawOutput(
            transaction.hash, transaction.time, output.toAddress, output.index, output.value,
            createJoinExpr(transaction.hash, output.index)
          ))
      }
  }

  private def transactionsToRawInputs(transactions: Dataset[SourceTransaction]): Dataset[RawInput] = {
    transactions
      .flatMap {
        case transaction: SourceTransaction if transaction.isCoinBase =>
          // We do not represent coinbase inputs as inputs
          Seq.empty

        case transaction: SourceTransaction =>
          transaction.inputs
            .map(input => RawInput(
              transaction.hash, transaction.time, input.fromAddress, input.index,
              input.outputTransaction, input.outputIndex,
              createJoinExpr(input.outputTransaction, input.outputIndex)
            ))
      }
  }

  private def rawToIO(inputs: Dataset[RawInput], outputs: Dataset[RawOutput]): Dataset[IO] = {
    inputs
      .joinWith(outputs, inputs("joinExpr") === outputs("joinExpr"), "full_outer")
      .flatMap {
        case (input, output) if Option(input).isDefined && Option(output).isDefined =>
          // Inputs which spent an output
          Seq(
            IO(input.transactionHash.hex, input.fromAddress orElse output.toAddress, output.value.map(-_), isInput = true, input.index, input.time),
            IO(output.transactionHash.hex, output.toAddress orElse input.fromAddress, output.value, isInput = false, output.index, output.time)
          )

        case (input, _) if Option(input).isDefined =>
          // Inputs for which we don't have the relevant outputs
          Seq(
            IO(input.transactionHash.hex, input.fromAddress, None, isInput = true, input.index, input.time)
          )

        case (_, output) =>
          // Outputs which we consider unspent
          Seq(
            IO(output.transactionHash.hex, output.toAddress, output.value, isInput = false, output.index, output.time)
          )
      }
  }

  implicit class TransactionReader(source: Dataset[SourceTransaction]) {
    import source.sparkSession.implicits._

    type Enriched = {
      val io: Dataset[IO]
      val transactions: Dataset[Transaction]
    }

    def enriched: Enriched = new {
      lazy val io: Dataset[IO] = {
        val rawInputs = transactionsToRawInputs(source)
        val rawOutputs = transactionsToRawOutputs(source)

        rawToIO(rawInputs, rawOutputs)
          .cache()
      }

      private lazy val fees: Dataset[TransactionFee] = {
        io
          .groupByKey(_.transactionHash)
          .flatMapGroups { (hash, iterator) =>
            val ios = iterator.toList

            if (ios.exists(_.value.isEmpty)) {
              Seq.empty[TransactionFee]
            } else {
              // Fee will be negative because sum of inputs > sum of outputs
              Seq(TransactionFee(hash.bytes, -1 * ios.flatMap(_.value).sum))
            }
          }
      }

      lazy val transactions: Dataset[Transaction] = {
        source
          .joinWith(fees, source("hash") === fees("hash"), "left_outer")
          .map { case (txData, txFee) =>
            val fee =
              if (txData.isCoinBase) {
                None
              } else {
                // fee may be null because of our left outer join
                Option(txFee).map(_.fee)
              }

            val sumOfOutputs =
              if (txData.outputs.exists(_.value.isEmpty)) {
                None
              } else {
                Option(txData.outputs.flatMap(_.value).sum)
              }

            Transaction(
              txData.blockHash.hex, txData.hash.hex, txData.isCoinBase, txData.time, txData.sizeInBytes, fee,
              sumOfOutputs
            )
          }
      }
    }
  }
}
