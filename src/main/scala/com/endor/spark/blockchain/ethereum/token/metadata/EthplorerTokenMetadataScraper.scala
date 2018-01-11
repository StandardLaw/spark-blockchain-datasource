package com.endor.spark.blockchain.ethereum.token.metadata

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsObject, JsPath, JsValue, Reads}
import play.api.libs.ws.JsonBodyReadables._
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.{ExecutionContext, Future}

class EthplorerTokenMetadataScraper(apiKey: String) extends TokenMetadataScraper {
  import EthplorerTokenMetadataScraper._

  private val wsClient = {
    val system = ActorSystem()
    val materializer = ActorMaterializer()(system)
    StandaloneAhcWSClient()(materializer)
  }

  override def scrapeAddress(address: String)
                            (implicit ec: ExecutionContext): Future[TokenMetadata] = {
    val url = s"https://api.ethplorer.io/getTokenInfo/0x$address?apiKey=$apiKey"
    wsClient.url(url).get()
      .map(_.body[JsValue].as[JsObject])
      .map(_.as[TokenMetadata].copy(address = address))
  }
}

object EthplorerTokenMetadataScraper {
  implicit val tokenMetadataReads: Reads[TokenMetadata] = (
    (JsPath \ "address").read[String] and
      (JsPath \ "symbol").readNullable[String] and
      (JsPath \ "totalSupply").readNullable[String] and
      (JsPath \ "decimals").readNullable[Int]
    )(TokenMetadata.apply _)
}
