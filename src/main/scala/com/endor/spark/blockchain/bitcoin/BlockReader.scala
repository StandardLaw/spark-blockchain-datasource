package com.endor.spark.blockchain.bitcoin

import java.io.InputStream
import java.nio.{ByteBuffer, ByteOrder}

import com.endor.spark.blockchain.bitcoin.BlockReader.EOF
import org.bitcoinj.core.{Block, MessageSerializer, NetworkParameters}

import scala.util.{Failure, Success, Try}

final class BlockReader(stream: InputStream, networkParameters: NetworkParameters) {
  // This logic is ripped from org.bitcoinj.utils.BlockFileLoader
  def nextBlock: Try[Either[EOF.type, Block]] = {
    // Figure out if the magic is happening
    readPossibleMagic()
      .flatMap {
        case Right(readMagic) if readMagic == networkParameters.getPacketMagic =>
          readPossibleLength()

        case Right(readMagic) =>
          Failure(new Exception(f"Bad magic $readMagic%02x, expected ${networkParameters.getPacketMagic}%02x"))

        case Left(_) =>
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

  private def readPossibleMagic(): Try[Either[EOF.type, Long]] = {
    val magicLength = Integer.BYTES
    val readBytes = new Array[Byte](magicLength)

    Try(stream.read(readBytes, 0, readBytes.length))
      .map {
        case count if count < readBytes.length => Left(EOF)
        case _ => Right {
          val padded: Array[Byte] = Array.fill(java.lang.Long.BYTES - readBytes.length)(0.toByte) ++ readBytes
          ByteBuffer.wrap(padded).order(ByteOrder.BIG_ENDIAN).getLong
        }
      }
  }
}

object BlockReader {
  case object EOF
}
