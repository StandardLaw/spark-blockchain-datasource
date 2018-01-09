package com.endor.spark.blockchain.ethereum.token

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.model.ElementNode
import org.apache.spark.sql.{Encoder, Encoders}

import scala.util.Try
import scala.util.matching.Regex

final case class TokenMetadata(address: String, symbol: String, totalSupply: String, decimals: Int)

object TokenMetadata {
  implicit val encoder: Encoder[TokenMetadata] = Encoders.product[TokenMetadata]
}

class TokenMetadataScraper() {
  private val browser = JsoupBrowser()
  private val symbolRegex: Regex = "totalSupply = (\\d+).*symbol = ([\\w\\d]+).*decimals = (\\d+).*".r("totalSupply", "symbol", "decimals")

  def scrapeAddress(address: String): Option[TokenMetadata] = {

    for {
      doc <- Try(browser.get(s"http://etherscan.io/tokens?q=0x$address")).toOption
      wrapper <- doc >> element("body") >?> element("div[class='wrapper']")
      container <- wrapper.childNodes
        .collect {
          case elem: ElementNode[_] =>
            elem.element
        }
        .find(elem => elem.hasAttr("class") && elem.attr("class") == "container")
      resultBody <- container >?> element("div[class='tag-box tag-box-v3']")
      matchResult <- symbolRegex.findFirstMatchIn(resultBody.text)
    } yield TokenMetadata(address, matchResult.group("symbol"),
      matchResult.group("totalSupply"), matchResult.group("decimals").toInt)
  }
}
