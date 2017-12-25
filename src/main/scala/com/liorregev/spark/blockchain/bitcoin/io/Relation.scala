package com.liorregev.spark.blockchain.bitcoin.io

import com.liorregev.spark.blockchain.bitcoin._
import org.apache.log4j.Logger
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.bitcoinj.core._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

final case class Relation(location: String, network: String)
                         (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  import Relation._

  override def schema: StructType = IO.encoder.schema

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
          logger.warn("Unable to decipher source address for input", ex)
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
        Option(ECKey.fromPublicOnly(script.getPubKey).toAddress(networkParameters).toString)

      } else if (script.isSentToMultiSig) {
        None // Multisig doesn't have an address

      } else {
        None // Unknown
      }
    } catch {
      case _: ScriptException =>
        // TODO: Log
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
          .min

        Transaction(
          block.getHash.getBytes, transaction.getHash.getBytes, transaction.isCoinBase,
          Option(transaction.getOutputSum.value),
          transaction.getInputs.asScala.zipWithIndex.map { case (input, index) =>
            Input(inputToAddress(input), index.toLong, input.getOutpoint.getHash.getBytes, input.getOutpoint.getIndex, None)
          },
          transaction.getOutputs.asScala.zipWithIndex.map { case (output, index) =>
            val maybeValue = Try(Option(output.getValue)).toOption.flatten.map(_.value)
            Output(outputToAddress(output), index.toLong, maybeValue)
          },
          time
        )
      }
  }

  override def buildScan(): RDD[Row] = {
    import sqlContext.implicits._

    val transactionParts: Dataset[Transaction] = sqlContext.sparkContext
      .binaryFiles(location)
      .flatMap {
        case (_: String, data: PortableDataStream) =>
          val reader = new BlockReader(data.open(), networkParameters)

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
      .toDS

    val inputs: Dataset[RawInput] = transactionParts
      .flatMap {
        case transaction: Transaction if transaction.isCoinBase =>
          // TODO: How do we represent CoinBase transactions?
          Seq.empty

        case transaction: Transaction =>
          transaction.inputs.map(input =>
            RawInput(transaction.blockHash, transaction.hash, transaction.blockTime, input.fromAddress, input.index, input.outputTransaction, input.outputIndex)
          )
      }
    
    val outputs: Dataset[RawOutput] = transactionParts
      .flatMap { (transaction: Transaction) =>
        transaction.outputs
          .map(output => RawOutput(transaction.blockHash, transaction.hash, transaction.blockTime, output.toAddress, output.index, output.value))
      }

    val io: Dataset[IO] = inputs
      .joinWith(outputs, inputs("outputTransaction") === outputs("transactionHash") and inputs("outputIndex") === outputs("index"), "full_outer")
      .flatMap {
        case (input, output) if Option(input).isDefined && Option(output).isDefined =>
          Seq(
            IO(input.blockHash, input.transactionHash, input.fromAddress orElse output.toAddress, output.value.map(-_), isInput = true, input.index, input.blockTime),
            IO(output.blockHash, output.transactionHash, output.toAddress orElse input.fromAddress, output.value, isInput = false, output.index, output.blockTime)
          )

        case (input, _) if Option(input).isDefined =>
          Seq(
            IO(input.blockHash, input.transactionHash, input.fromAddress, None, isInput = true, input.index, input.blockTime)
          )

        case (_, output) =>
          Seq(
            IO(output.blockHash, output.transactionHash, output.toAddress, output.value, isInput = false, output.index, output.blockTime)
          )
      }

    io
      .rdd
      .map((otherIO: IO) => Row.fromTuple(otherIO))
  }
}

object Relation {
  private[Relation] final case class Input(fromAddress: Option[String], index: Long,
                                           outputTransaction: Hash, outputIndex: Long,
                                           percentageOfTransaction: Option[Double])

  object Input {
    implicit lazy val encoder: Encoder[Input] = Encoders.product[Input]
  }

  private[Relation] final case class Output(toAddress: Option[String], index: Long, value: Option[Long])

  object Output {
    implicit lazy val encoder: Encoder[Output] = Encoders.product[Output]
  }

  private[Relation] final case class Transaction(blockHash: Hash, hash: Hash, isCoinBase: Boolean,
                                                 value: Option[Long],
                                                 inputs: Seq[Input], outputs: Seq[Output],
                                                 blockTime: Long)

  object Transaction {
    implicit lazy val encoder: Encoder[Transaction] = Encoders.product[Transaction]
  }

  private[Relation] final case class RawInput(blockHash: Hash, transactionHash: Hash, blockTime: Long,
                                              fromAddress: Option[String], index: Long,
                                              outputTransaction: Hash, outputIndex: Long)

  private[Relation] object RawInput {
    implicit lazy val encoder: Encoder[RawInput] = Encoders.product[RawInput]
  }

  private[Relation] final case class RawOutput(blockHash: Hash, transactionHash: Hash, blockTime: Long,
                                               toAddress: Option[String], index: Long, value: Option[Long])

  private[Relation] object RawOutput {
    implicit lazy val encoder: Encoder[RawOutput] = Encoders.product[RawOutput]
  }
}

