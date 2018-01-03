package com.endor.spark.blockchain.bitcoin

import com.endor.spark.blockchain._
import com.endor.spark.blockchain.bitcoin.transactions.Transaction
import org.apache.spark.sql.Dataset

package object io {
  private def createJoinExpr(transaction: Hash, index: Long): String = {
    s"${transaction.hex}-$index"
  }

  private def transactionsToRawOutputs(transactions: Dataset[Transaction]): Dataset[RawOutput] = {
    transactions
      .flatMap { (transaction: Transaction) =>
        transaction.outputs
          .map(output => RawOutput(
            transaction.blockHash, transaction.hash, transaction.time, output.toAddress, output.index, output.value,
            createJoinExpr(transaction.hash, output.index)
          ))
      }
  }

  private def transactionsToRawInputs(transactions: Dataset[Transaction]): Dataset[RawInput] = {
    transactions
      .flatMap {
        case transaction: Transaction if transaction.isCoinBase =>
          // We do not represent coinbase inputs as inputs
          Seq.empty

        case transaction: Transaction =>
          transaction.inputs
            .map(input => RawInput(
              transaction.blockHash, transaction.hash, transaction.time, input.fromAddress, input.index,
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
            IO(input.blockHash, input.transactionHash, input.fromAddress orElse output.toAddress, output.value.map(-_), isInput = true, input.index, input.time),
            IO(output.blockHash, output.transactionHash, output.toAddress orElse input.fromAddress, output.value, isInput = false, output.index, output.time)
          )

        case (input, _) if Option(input).isDefined =>
          // Inputs for which we don't have the relevant outputs
          Seq(
            IO(input.blockHash, input.transactionHash, input.fromAddress, None, isInput = true, input.index, input.time)
          )

        case (_, output) =>
          // Outputs which we consider unspent
          Seq(
            IO(output.blockHash, output.transactionHash, output.toAddress, output.value, isInput = false, output.index, output.time)
          )
      }
  }

  implicit class TransactionReader(val transactions: Dataset[Transaction]) extends AnyVal {
    def toIO: Dataset[IO] = {
      val inputs = transactionsToRawInputs(transactions)
      val outputs = transactionsToRawOutputs(transactions)
      rawToIO(inputs, outputs)
    }
  }
}
