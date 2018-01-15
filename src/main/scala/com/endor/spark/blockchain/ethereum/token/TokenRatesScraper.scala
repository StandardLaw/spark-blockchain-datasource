package com.endor.spark.blockchain.ethereum.token

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import org.apache.spark.sql.{Encoder, Encoders}

final case class TokenRate(token: String, date: java.sql.Date, open: Double, high: Double, low: Double,
                           close: Double, volume: Long, marketCap: Long)

object TokenRate {
  implicit val encoder: Encoder[TokenRate] = Encoders.product[TokenRate]
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
          case List(date, open, high, low, close, volume, marketCap) =>
            val parsedDate = parser.parse(date)
            TokenRate(slug, new java.sql.Date(parsedDate.getTime), open.toDouble, high.toDouble, low.toDouble,
              close.toDouble, volume.replace(",", "").toLong, marketCap.replace(",", "").toLong)
        }
      })
  }

  private def createUrl(slug: String, start: Instant, end: Instant) =
    s"https://coinmarketcap.com/currencies/$slug/historical-data/?start=${formatter.format(start)}&end=${formatter.format(end)}"
}
