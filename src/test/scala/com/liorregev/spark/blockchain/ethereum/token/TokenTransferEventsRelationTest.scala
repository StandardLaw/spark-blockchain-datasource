package com.liorregev.spark.blockchain.ethereum.token

import java.util.concurrent.TimeUnit

import com.liorregev.spark.blockchain._
import com.liorregev.spark.blockchain.ethereum._
import com.liorregev.spark.blockchain.ethereum.block.EthereumTestUtils
import org.apache.http.entity.StringEntity
import org.apache.http.localserver.LocalServerTestBase
import org.apache.http.message.BasicHttpEntityEnclosingRequest
import org.apache.http.protocol.HttpContext
import org.apache.http.{HttpRequest, HttpResponse}
import org.apache.spark.sql.{SparkSession, functions => F}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.request.EthFilter
import org.web3j.protocol.core.methods.response.EthLog.LogObject
import org.web3j.protocol.http.HttpService
import play.api.libs.json._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

private final case class RawEvent(address: String, topics: Seq[String], data: String, blockNumber: Long,
                            transactionHash: String, transactionIndex: String, blockHash: String,
                            logIndex: String, removed: Boolean)

private object RawEvent {
  @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
  implicit val writes: OWrites[RawEvent] = (o: RawEvent) =>
    Json.writes[RawEvent].writes(o) + ("blockNumber" -> JsString("0x" + o.blockNumber.toHexString))
}

private class LocalTestServer() extends LocalServerTestBase() {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var registeredEvents: ListBuffer[RawEvent] = ListBuffer.empty

  def addEvent(event: RawEvent): Unit = {
    registeredEvents.append(event)
  }

  override def shutDown(): Unit = {
    if (this.httpclient != null) {
      this.httpclient.close()
    }
    if (this.server != null) {
      this.server.shutdown(0, TimeUnit.SECONDS)
    }
  }


  @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
  override def setUp(): Unit = {
    super.setUp()
    registeredEvents = ListBuffer.empty
    serverBootstrap.registerHandler("/", (request: HttpRequest, response: HttpResponse, _: HttpContext) => {
      request match {
        case entityRequest: BasicHttpEntityEnclosingRequest =>
          val requestBody = Json.parse(entityRequest.getEntity.getContent).as[Map[String, JsValue]]
          val relevantEvents = for {
            method <- requestBody.get("method")
            allParams <- method.as[String] match {
              case "eth_getLogs" =>
                requestBody.get("params").map(_.as[Seq[JsObject]])
              case _ => None
            }
            paramsObject <- allParams.headOption.map(_.as[Map[String, JsValue]])
            topic <- paramsObject.get("topics").flatMap(_.as[Seq[String]].headOption)
            fromBlock <- paramsObject.get("fromBlock").map(_.as[String].hexToLong)
            toBlock <- paramsObject.get("toBlock").map(_.as[String].hexToLong)
            results <- topic match {
              case TokenTransferEvent.topic =>
                Option(registeredEvents.filter(_.blockNumber >= fromBlock).filter(_.blockNumber <= toBlock))
              case _ => None
            }
          } yield results
          relevantEvents.foreach(events => {
            val entityBody = JsObject(Seq(
              "jsonrpc" -> JsString("2.0"),
              "id" -> JsNumber(0),
              "result" -> Json.toJson(events)
            ))
            response.setEntity(new StringEntity(entityBody.toString))
            response.setStatusCode(200)
          })
      }
    })
  }
}

class TokenTransferEventsRelationTest extends FunSuite with EthereumTestUtils
  with Matchers with BeforeAndAfterEach {

  private val mockServer = new LocalTestServer()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    mockServer.setUp()
    mockServer.addEvent(RawEvent(
      "0x1776e1f26f98b1a5df9cd347953a26dd3cb46671",
      Seq("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", "0x000000000000000000000000638141cfe7c64fe9a22400e7d9f682d5f7b3a99b", "0x0000000000000000000000000000000000000000000000000000000000003689"),
      "0x000000000000000000000000000000000000000000000047ffb0765e72b6bc00",
      3904411L,
      "0x64bab5195bcef2fd334f8d7b7cbefb2810c66460f566e8262435de8866410244",
      "0x0",
      "0xf5f12d939472b79009f86163f6ec4440ed067fd14f222bc8e9cc9b82cdbaa71b",
      "0x0",
      removed = false
    ))
  }


  override protected def afterEach(): Unit = {
    super.afterEach()
    mockServer.shutDown()
  }

  private val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  test("Base parse test") {
    val expected = TokenTransferEvent(
      "1776e1f26f98b1a5df9cd347953a26dd3cb46671".bytes,
      "638141cfe7c64fe9a22400e7d9f682d5f7b3a99b".bytes,
      "0000000000000000000000000000000000003689".bytes,
      1328143185456974445568.0,
      3904411L,
      "64bab5195bcef2fd334f8d7b7cbefb2810c66460f566e8262435de8866410244".bytes,
      0
    )

    val host = mockServer.start()
    val web3j = Web3j.build(new HttpService(s"http://${host.getHostName}:${host.getPort}/"))
    val filter = new EthFilter(
      DefaultBlockParameter.valueOf(BigInt(3904411).bigInteger),
      DefaultBlockParameter.valueOf(BigInt(3904411).bigInteger),
      (Nil: List[String]).asJava
    ).addSingleTopic(TokenTransferEvent.topic)
    val event = web3j.ethGetLogs(filter).send()
      .getLogs.asScala
      .headOption
      .collect {
        case log: LogObject => log
      }.flatMap(TokenTransferEvent.fromEthLog)
    event shouldBe defined
    event.foreach(x => x should equal (expected))
  }

  test("Simple spark test") {
    val expected = TokenTransferEvent(
      "1776e1f26f98b1a5df9cd347953a26dd3cb46671".bytes,
      "638141cfe7c64fe9a22400e7d9f682d5f7b3a99b".bytes,
      "0000000000000000000000000000000000003689".bytes,
      1328143185456974445568.0,
      3904411L,
      "64bab5195bcef2fd334f8d7b7cbefb2810c66460f566e8262435de8866410244".bytes,
      0
    )
    val host = mockServer.start()
    val parsed = spark.read.tokenTransferEvents(3904411, 3904411, s"${host.getHostName}:${host.getPort}")
      .where(F.col("transactionIndex") equalTo 0)
      .head
    parsed should equal(expected)
  }
}
