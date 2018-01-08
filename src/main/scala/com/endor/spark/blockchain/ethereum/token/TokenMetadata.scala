package com.endor.spark.blockchain.ethereum.token

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex

final case class TokenMetadata(address: String, symbol: String, totalSupply: String)

object TokenMetadata {
  private val symbolRegex: Regex = "totalSupply = (\\d+).*symbol = ([\\w\\d]+).*".r("totalSupply", "symbol")
  private lazy val wsClient = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    StandaloneAhcWSClient()
  }

  def getDataForAddress(address: String)(implicit ex: ExecutionContext): Future[Option[TokenMetadata]] = {
    wsClient.url(s"http://etherscan.io/tokens?q=$address").get()
      .map(response => response.body)
      .map(symbolRegex.findFirstMatchIn)
      .map(maybeMatch => maybeMatch.map(actualMatch =>
        TokenMetadata(address, actualMatch.group("symbol"), actualMatch.group("totalSupply"))))
  }
}
