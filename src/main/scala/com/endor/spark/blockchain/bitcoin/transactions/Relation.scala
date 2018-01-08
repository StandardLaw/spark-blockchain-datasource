package com.endor.spark.blockchain.bitcoin.transactions

import java.io.ByteArrayInputStream

import org.apache.log4j.Logger
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.bitcoinj.core._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

final case class Relation(location: String, network: String)
                         (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = Transaction.encoder.schema

  private lazy val logger: Logger = org.apache.log4j.LogManager.getLogger(this.getClass)

  private lazy val networkParameters: NetworkParameters = {
    val params = NetworkParameters.fromPmtProtocolID(network)
    new Context(params) // Manually initialize a context for this thread if one does not exist
    params
  }

  private def inputToAddress(input: TransactionInput): Option[String] = {
    if (input.isCoinBase) {
      None
    } else {
      try {
        Option(new Address(networkParameters, Utils.sha256hash160(input.getScriptSig.getPubKey)).toString)
      } catch {
        case ex: ScriptException =>
          val possiblyIndex = input.getParentTransaction.getInputs.asScala
            .zipWithIndex
            .collectFirst { case (transactionInput, index) if transactionInput == input => index.toString }
            .getOrElse("?")

          logger.warn(s"Unable to decipher address for transaction ${input.getParentTransaction.getHashAsString} input #$possiblyIndex", ex)
          None
      }
    }
  }

  private def outputToAddress(output: TransactionOutput): Option[String] = {
    try {
      val script = output.getScriptPubKey

      if (script.isSentToAddress || script.isPayToScriptHash) {
        Option(script.getToAddress(networkParameters).toString)

      } else if (script.isSentToRawPubKey) {
        val attemptedAddress = for {
          ecKey <- Try(ECKey.fromPublicOnly(script.getPubKey))
          address <- Try(ecKey.toAddress(networkParameters))
          addressRepr <- Try(address.toString)
        } yield addressRepr

        attemptedAddress match {
          case Failure(ex) =>
            logger.warn(s"Unable to decipher address for transaction ${output.getParentTransactionHash.toString} output #${output.getIndex}", ex)
            None
          case Success(address) =>
            Option(address)
        }

      } else if (script.isSentToMultiSig) {
        None // Multisig doesn't have an address

      } else {
        None // Unknown
      }
    } catch {
      case ex: ScriptException =>
        logger.warn(s"Unable to decode transaction ${output.getParentTransactionHash.toString} output #${output.getIndex} address", ex)
        None
    }
  }

  private def blockToTransactions(block: Block): Seq[Transaction] = {
    block.getTransactions.asScala
      .map { transaction =>
        val actualLockTime =
          // If the lock time is before the Genesis Block's time, it's probably specifying a block height and not a time
          if (transaction.getLockTime >= 1230999305000L) {
            transaction.getLockTime
          } else {
            0L
          }

        // Estimate the time
        val time = Seq(transaction.getUpdateTime.getTime, actualLockTime, block.getTimeSeconds * 1000)
          .filter(_ > 0L)
          .sorted
          .headOption

        Transaction(
          block.getHash.getBytes, transaction.getHash.getBytes, transaction.isCoinBase,
          transaction.getInputs.asScala.zipWithIndex.map { case (input, index) =>
            Input(inputToAddress(input), index.toLong, input.getOutpoint.getHash.getBytes, input.getOutpoint.getIndex)
          },
          transaction.getOutputs.asScala.zipWithIndex.map { case (output, index) =>
            val maybeValue = Try(Option(output.getValue)).toOption.flatten.map(_.value)
            Output(outputToAddress(output), index.toLong, maybeValue)
          },
          time,
          transaction.getMessageSize
        )
      }
  }

  override def buildScan(): RDD[Row] = {
    val transactions: RDD[Transaction] = sqlContext.sparkContext
      .binaryFiles(location)
      .flatMap {
        case (_: String, data: PortableDataStream) =>
          val reader = new BlockReader(new ByteArrayInputStream(data.toArray()), networkParameters)

          Stream.continually(reader.nextBlock)
            .takeWhile {
              case Success(Left(BlockReader.EOF)) =>
                false
              case Failure(ex) =>
                logger.error("Error while deserializing block", ex)
                false
              case _ =>
                true
            }
            .collect {
              case Success(Right(block)) => blockToTransactions(block)
            }
            .flatten
      }

    transactions
      .map((transaction: Transaction) => Row.fromTuple(transaction))
  }
}
