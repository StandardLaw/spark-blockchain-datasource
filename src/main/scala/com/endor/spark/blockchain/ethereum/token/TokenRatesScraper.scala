package com.endor.spark.blockchain.ethereum.token

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import org.apache.spark.sql.{Encoder, Encoders}
import play.api.libs.json._
import play.api.libs.functional.syntax._

final case class TokenRate(token: String, date: Date, open: Double, high: Double, low: Double,
                           close: Double, marketCap: Long)

object TokenRate {
  implicit lazy val encoder: Encoder[TokenRate] = Encoders.product[TokenRate]

  implicit lazy val format: OFormat[TokenRate] = {
    val reads = (
      (__ \ "token").read[String] and
        (__ \ "date").read[String].map(Date.valueOf) and
        (__ \ "open").read[Double] and
        (__ \ "high").read[Double] and
        (__ \ "low").read[Double] and
        (__ \ "close").read[Double] and
        (__ \ "marketCap").read[Long]
      )(TokenRate.apply _)

    @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
    val writes: OWrites[TokenRate] = (o: TokenRate) =>  JsObject(Map(
      "token" -> JsString(o.token),
      "date" -> JsString(o.date.toString),
      "open" -> JsNumber(o.open),
      "high" -> JsNumber(o.high),
      "low" -> JsNumber(o.low),
      "close" -> JsNumber(o.close),
      "marketCap" -> JsNumber(o.marketCap)
    ))
    OFormat(reads, writes)
  }

  def fromSingleRate(token: String, date: Date, rate: Double, marketCap: Long): TokenRate =
    TokenRate(token, date, rate, rate, rate, rate, marketCap)
}

class TokenRatesScraper() {
  private val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    .withZone(ZoneId.systemDefault())

  def scrapeToken(slug: String, start: Instant, end: Instant): Seq[TokenRate] = {
    Seq(createUrl(slug, start, end))
      .flatMap((url: String) => {
        val browser = JsoupBrowser()
        val parser = new SimpleDateFormat("MMM dd, yyyy")
        val doc = browser.get(url)
        val table = doc >> element("div[class='table-responsive']") >> element("table")

        table >> element("tbody") >> elementList("tr") map {
          row =>
            (row >> elementList("td"))
              .map(_.text)
        } map {
          case List(date, open, high, low, close, _, marketCap) =>
            val parsedDate = parser.parse(date)
            TokenRate(slug, new java.sql.Date(parsedDate.getTime), open.toDouble, high.toDouble, low.toDouble,
              close.toDouble, marketCap.replace(",", "").toLong)
        }
      })
  }

  private def createUrl(slug: String, start: Instant, end: Instant) =
    s"https://coinmarketcap.com/currencies/$slug/historical-data/?start=${formatter.format(start)}&end=${formatter.format(end)}"
}
