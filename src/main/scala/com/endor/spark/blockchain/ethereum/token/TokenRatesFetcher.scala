package com.endor.spark.blockchain.ethereum.token

import java.sql.Date
import java.time.LocalDate

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.json.{JsObject, JsValue}
import play.api.libs.ws.JsonBodyReadables._
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.{ExecutionContext, Future}

class TokenRatesFetcher()(implicit system: ActorSystem) {
  private val wsClient = StandaloneAhcWSClient()(ActorMaterializer())

  def fetchRate(token: String)
               (implicit ex: ExecutionContext): Future[TokenRate] = {
    val today = Date.valueOf(LocalDate.now())
    wsClient.url(s"https://min-api.cryptocompare.com/data/price?fsym=$token&tsyms=USD")
      .get()
      .map(_.body[JsValue].as[JsObject].as[Map[String, JsValue]])
      .map {
        result =>
          for {
            rate <- result.get("USD")
            rateDouble <- rate.asOpt[Double]
          } yield TokenRate.fromSingleRate(token, today, rateDouble)
      }
      .flatMap {
        case Some(rate) => Future.successful(rate)
        case None => Future.failed(new Exception("Could not fetch rate"))
      }
  }
}