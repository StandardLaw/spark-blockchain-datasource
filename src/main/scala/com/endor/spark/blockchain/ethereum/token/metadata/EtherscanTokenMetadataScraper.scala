package com.endor.spark.blockchain.ethereum.token.metadata

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import ch.qos.logback.classic.{Logger, LoggerContext}
import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.model.ElementNode
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.element
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex

class EtherscanTokenMetadataScraper()
                                   (implicit system: ActorSystem, loggerFactory: LoggerContext)
  extends TokenMetadataScraper {
  private lazy val logger: Logger = loggerFactory.getLogger(this.getClass)
  private val browser = JsoupBrowser()
  private val wsClient = {
    val materializer = ActorMaterializer()
    StandaloneAhcWSClient()(materializer)
  }
  private val symbolRegex: Regex = "totalSupply = (\\d+).*symbol = ([\\w\\d]+).*decimals = (\\d+).*".r("totalSupply", "symbol", "decimals")

  def scrapeAddress(address: String)
                   (implicit ec: ExecutionContext): Future[TokenMetadata] = {
    logger.debug(s"Scraping etherscan for $address")
    wsClient.url(s"http://etherscan.io/tokens?q=0x$address").get()
      .map(_.body)
      .map(browser.parseString)
      .map(_ >> element("body") >> element("div[class='wrapper']"))
      .map(_.childNodes
        .collect {
          case elem: ElementNode[_] if elem.element.hasAttr("class") && elem.element.attr("class") == "container" =>
            elem.element
        }
      )
      .map(_.headOption.map(_ >> element("div[class='tag-box tag-box-v3']")))
      .map(_.flatMap(resultBody => symbolRegex.findFirstMatchIn(resultBody.text)))
      .flatMap {
        case Some(matchResult) => Future.successful(
          TokenMetadata.fromConcrete(address, matchResult.group("symbol"),
            matchResult.group("totalSupply"), matchResult.group("decimals").toInt)
        )
        case None => Future.failed(new Exception("No regex match"))
      }
  }
}
