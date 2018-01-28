package com.endor.spark.blockchain.ethereum.token.metadata

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import ch.qos.logback.classic.{Logger, LoggerContext}
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.ws.JsonBodyReadables._
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.{ExecutionContext, Future}

class EthplorerTokenMetadataScraper(apiKey: String)
                                   (implicit system: ActorSystem, loggerFactory: LoggerContext)
  extends TokenMetadataScraper {
  private lazy val logger: Logger = loggerFactory.getLogger(this.getClass)
  import EthplorerTokenMetadataScraper._

  private val wsClient = StandaloneAhcWSClient()(ActorMaterializer())

  override def scrapeAddress(address: String)
                            (implicit ec: ExecutionContext): Future[TokenMetadata] = {
    logger.debug(s"Scraping ethplorer for $address")
    val url = s"https://api.ethplorer.io/getTokenInfo/0x$address?apiKey=$apiKey"
    wsClient.url(url).get()
      .map(_.body[JsValue].as[JsObject])
      .map(_.as[TokenMetadata].copy(address = address))
  }
}

object EthplorerTokenMetadataScraper {
  implicit val tokenMetadataReads: Reads[TokenMetadata] = (
    (__ \ "address").read[String] and
      (__ \ "name").read[String].orElse(Reads.pure("")) and
      (__ \ "symbol").read[String].orElse(Reads.pure("")) and
      (__ \ "totalSupply").read[String].orElse(Reads.pure("")) and
      (__ \ "decimals").readNullable[Int]
    )(TokenMetadata.fromConcrete _)
}
