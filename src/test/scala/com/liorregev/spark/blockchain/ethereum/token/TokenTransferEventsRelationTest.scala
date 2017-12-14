package com.liorregev.spark.blockchain.ethereum.token

import java.util.concurrent.TimeUnit

import com.liorregev.spark.blockchain._
import com.liorregev.spark.blockchain.ethereum._
import com.liorregev.spark.blockchain.ethereum.block.EthereumTestUtils
import org.apache.http.entity.StringEntity
import org.apache.http.localserver.LocalServerTestBase
import org.apache.http.message.BasicHttpEntityEnclosingRequest
import org.apache.http.protocol.HttpContext
import org.apache.http.{HttpHost, HttpRequest, HttpResponse}
import org.apache.spark.sql.SparkSession
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
  implicit val format: OFormat[RawEvent] = new OFormat[RawEvent] {
    private val baseWrites = Json.writes[RawEvent]
    private val baseReads = Json.reads[RawEvent]

    override def reads(json: JsValue): JsResult[RawEvent] = json match {
      case obj: JsObject =>
        val blockNumber = {
          for {
            hexBlockNumber <- obj.value.get("blockNumber")
            num <- hexBlockNumber match {
              case JsString(value) => Option(java.lang.Long.parseLong(value.drop(2), 16))
              case _ => None
            }
          } yield num
        }
        blockNumber
          .map(blockNum => baseReads.reads(obj + ("blockNumber" -> JsNumber(blockNum))))
          .getOrElse(JsError(__ \ "blockNumber", "Missing field in data"))
      case _ => JsError("Data is not an object")
    }

    override def writes(o: RawEvent): JsObject =
      baseWrites.writes(o) + ("blockNumber" -> JsString("0x" + o.blockNumber.toHexString))
  }
}

class TokenTransferEventsRelationTest extends FunSuite with EthereumTestUtils
  with Matchers with BeforeAndAfterEach {

  private class LocalTestServer() extends LocalServerTestBase() {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var numCalls = 0

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var registeredEvents: ListBuffer[RawEvent] = ListBuffer.empty

    def addEvents(eventNames: String*): Unit = {
      registeredEvents ++=
        eventNames.map(eventName => {
          val eventPath = s"/com/liorregev/spark/blockchain/ethereum/events/$eventName.json"
          Json.parse(this.getClass.getResourceAsStream(eventPath)).as[RawEvent]
        })
    }

    def getNumCalls: Int = numCalls

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
      numCalls = 0
      registeredEvents = ListBuffer.empty
      serverBootstrap.registerHandler("/", (request: HttpRequest, response: HttpResponse, _: HttpContext) => {
        numCalls += 1
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

  implicit def httpHostToString(host: HttpHost): String = s"${host.getHostName}:${host.getPort}"

  private val mockServer = new LocalTestServer()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    mockServer.setUp()
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

    mockServer.addEvents("test_event_1")
    val host = mockServer.start()
    val web3j = Web3j.build(new HttpService(s"http://${httpHostToString(host)}/"))
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
    mockServer.addEvents("test_event_1")
    val host = mockServer.start()
    val parsed = spark.read.tokenTransferEvents(3904411, 3904411, host).head
    parsed should equal(expected)
  }

  test("Using 2 hosts") {
    val expected = Seq(
      TokenTransferEvent(
        "1776e1f26f98b1a5df9cd347953a26dd3cb46671".bytes,
        "638141cfe7c64fe9a22400e7d9f682d5f7b3a99b".bytes,
        "0000000000000000000000000000000000003689".bytes,
        1328143185456974445568.0,
        3904411L,
        "64bab5195bcef2fd334f8d7b7cbefb2810c66460f566e8262435de8866410244".bytes,
        0
      ),
      TokenTransferEvent(
        "4fe6ea636abe664e0268af373a10ca3621a0b95b".bytes,
        "2984581ece53a4390d1f568673cf693139c97049".bytes,
        "d5f035581b3f86edb225c99e69d2790f027cf928".bytes,
        4990000000000.0,
        3904412L,
        "ad06b153c84e05380b32327e46f963833b0b8fa880954f52a2b9ea3ecb4f1537".bytes,
        32
      )
    )

    val mockServer2 = new LocalTestServer()
    mockServer2.setUp()
    Seq(mockServer, mockServer2).foreach(s => {
      s.addEvents("test_event_1", "test_event_2")
    })

    val host1 = mockServer.start()
    val host2 = mockServer2.start()
    val parsed = spark.read.tokenTransferEvents(3904411, 3904412, host1, host2).collect()
    mockServer2.shutDown()
    mockServer.getNumCalls should equal(1)
    mockServer2.getNumCalls should equal(1)
    parsed should contain theSameElementsAs expected
  }

  test("Using 2 hosts with one partition") {
    val expected = Seq(
      TokenTransferEvent(
        "1776e1f26f98b1a5df9cd347953a26dd3cb46671".bytes,
        "638141cfe7c64fe9a22400e7d9f682d5f7b3a99b".bytes,
        "0000000000000000000000000000000000003689".bytes,
        1328143185456974445568.0,
        3904411L,
        "64bab5195bcef2fd334f8d7b7cbefb2810c66460f566e8262435de8866410244".bytes,
        0
      ),
      TokenTransferEvent(
        "4fe6ea636abe664e0268af373a10ca3621a0b95b".bytes,
        "2984581ece53a4390d1f568673cf693139c97049".bytes,
        "d5f035581b3f86edb225c99e69d2790f027cf928".bytes,
        4990000000000.0,
        3904412L,
        "ad06b153c84e05380b32327e46f963833b0b8fa880954f52a2b9ea3ecb4f1537".bytes,
        32
      )
    )

    val mockServer2 = new LocalTestServer()
    mockServer2.setUp()
    Seq(mockServer, mockServer2).foreach(s => {
      s.addEvents("test_event_1", "test_event_2")
    })

    val host1 = mockServer.start()
    val host2 = mockServer2.start()
    val parsed = spark.read.option("numPartitions", "1").tokenTransferEvents(3904411, 3904412, host1, host2).collect()
    mockServer2.shutDown()
    mockServer.getNumCalls should equal(1)
    mockServer2.getNumCalls should equal(0)
    parsed should contain theSameElementsAs expected
  }

  test("Given enough blocks, the result Dataset should have `numPartitions` partitions") {
    val result = spark.read.option("numPartitions", "132")
      .tokenTransferEvents(3904411, 3904411 + 132 * 3, mockServer.start())
    result.rdd.getNumPartitions should equal(132)
  }

  test("Given less than enough blocks, the result Dataset should have `numBlocks + 1` partitions") {
    val fromBlock = 3904411L
    val toBlock = fromBlock + 20
    val result = spark.read.option("numPartitions", "132")
      .tokenTransferEvents(fromBlock, toBlock, mockServer.start())
    result.rdd.getNumPartitions should equal(toBlock - fromBlock + 1)
  }
}
