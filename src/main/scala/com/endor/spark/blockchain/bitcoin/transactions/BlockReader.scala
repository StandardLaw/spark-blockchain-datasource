package com.endor.spark.blockchain.bitcoin.transactions

import java.io.InputStream
import java.nio.{ByteBuffer, ByteOrder}

import org.bitcoinj.core.{Block, MessageSerializer, NetworkParameters}

import scala.util.{Failure, Success, Try}

final class BlockReader(stream: InputStream, networkParameters: NetworkParameters) {
  import BlockReader._

  // This logic is ripped from org.bitcoinj.utils.BlockFileLoader
  def nextBlock: Try[Either[EOF.type, Block]] = {
    // Figure out if the magic is happening
    verifyPossibleMagic()
      .flatMap {
        case None =>
          readPossibleLength()

        case Some(EOF) =>
          Success(Left(EOF))
      }
      .flatMap {
        case Right(blockLength) =>
          readBlockDataFromStream(blockLength, networkParameters.getDefaultSerializer)
            .map(Right[EOF.type, Block])

        case Left(_) =>
          Success(Left(EOF))
      }
  }

  private def readBlockDataFromStream(blockLength: Int, messageSerializer: MessageSerializer): Try[Block] = {
    val readBytes = new Array[Byte](blockLength)
    Try(stream.read(readBytes, 0, readBytes.length).ensuring(_ == readBytes.length))
      .map(_ => messageSerializer.makeBlock(readBytes))
  }

  private def readPossibleLength(): Try[Either[EOF.type, Int]] = {
    val readBytes = new Array[Byte](Integer.BYTES)
    Try(stream.read(readBytes, 0, readBytes.length))
      .map {
        case count if count < readBytes.length => Left(EOF)
        case _ => Right {
          ByteBuffer.wrap(readBytes.reverse).order(ByteOrder.BIG_ENDIAN).getInt
        }
      }
  }

  private def verifyPossibleMagic(): Try[Option[EOF.type]] = {
    val expectedMagic = networkParameters.getPacketMagic.toInt
    val magicLength = Integer.BYTES
    val firstByteOfMagic = ByteBuffer.allocate(magicLength).order(ByteOrder.BIG_ENDIAN).putInt(expectedMagic).get(0).toInt & 0xff

    val possiblyActualMagic: Try[Either[EOF.type, Int]] = Try {
      val eofOrHeadOfMagic = Stream.continually(stream.read())
        .dropWhile(byte => byte != -1 && byte != firstByteOfMagic)
        .headOption

      eofOrHeadOfMagic match {
        case Some(-1) =>
          Left(EOF)

        case _ =>
          val readBytes = new Array[Byte](magicLength)
          readBytes(0) = firstByteOfMagic.toByte

          stream.read(readBytes, 1, readBytes.length - 1) match {
            case count if count < readBytes.length - 1 =>
              Left(EOF)

            case _ =>
              Right(ByteBuffer.wrap(readBytes).order(ByteOrder.BIG_ENDIAN).getInt)
          }
      }
    }

    possiblyActualMagic match {
      case Success(Left(_)) =>
        Success(Option(EOF))
      case Success(Right(actualMagic)) if actualMagic == expectedMagic =>
        Success(None)
      case Success(Right(actualMagic)) =>
        Failure(new Exception(f"Bad magic $actualMagic%02x, expected $expectedMagic%02x"))
      case Failure(ex) =>
        Failure(ex)
    }
  }
}

object BlockReader {
  case object EOF
}
