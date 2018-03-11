package com.endor.spark.blockchain.ethereum.token.logsfetcher

import java.io.{File, FileWriter}

import com.endor.spark.blockchain.ethereum.token.EthLogsParser
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.request.EthFilter
import org.web3j.protocol.core.methods.response.EthLog.LogObject
import org.web3j.protocol.http.HttpService
import org.web3j.protocol.ipc.{IpcService, UnixDomainSocket}
import play.api.libs.json._
import scopt.OptionParser

import scala.collection.JavaConverters._
import scala.reflect.io.Path
import scala.util.Try



sealed trait Command

object Command {
  sealed trait CommunicationMode
  object CommunicationMode {
    val defaultIpcPath: Path = Path(System.getProperty("user.home")) / Path(".ethereum") / Path("geth.ipc")

    final case class Http(url: String) extends CommunicationMode
    final case class IPC(path: Path) extends CommunicationMode

    implicit val communicationModeRead: scopt.Read[CommunicationMode] =
      scopt.Read.reads[CommunicationMode] {
        case inp if inp.startsWith("ipc") => IPC(Path(inp.split(" ").last))
        case inp if inp.startsWith("http") => Http(inp.split(" ").last)
      }
  }

  final case class Fetch(communicationMode: CommunicationMode = CommunicationMode.IPC(CommunicationMode.defaultIpcPath),
                         fromBlock: DefaultBlockParameter = DefaultBlockParameter.valueOf("earliest"),
                         toBlock: DefaultBlockParameter = DefaultBlockParameter.valueOf("latest"),
                         output: File = new File("logs.out"),
                         topics: Seq[String] = Nil) extends Command

  case object NoOp extends Command

  implicit val blockParameterRead: scopt.Read[DefaultBlockParameter] =
    scopt.Read.reads[DefaultBlockParameter] {
      inp =>
        Try {
          val inpLong = inp.toLong
          DefaultBlockParameter.valueOf(BigInt(inpLong).bigInteger)
        } getOrElse DefaultBlockParameter.valueOf(inp)
    }
}

object LogsFetcher extends App {
  import Command._

  private val logo =
    """
      |  █████████▓    ╫██▌_       ╟██▌ ╫████████▓▄▄_        ╓▄▓██████▄▄_ ╙▒████████▓▄,
      |  ███▀"'""╙     ▒████▄      ╟██▌ ╫██▌▀▀▀▀▀▀████,    ▄███▀▀^^"▀▀███▄_ ╙▀▀▀▀▀▀▀████
      |  ███           ▒██▀███▄    ╟██▌ ╫██▌       ╙███▄  ▒██▀         ▀███          ▒██▌
      |  ███▄▄▄▄▄▄╕    ▒██L`▀██▌_  ╟██▌ ╫██▌        "███ ╟██▓           ▒██▌       _╓███^
      |  ███▀▀▀▀▀▀▀    ▒██L  ╙███▄ ╟██▌ ╫██▌         ███ ╟██▌           ╫██▌    ▒█████▀
      |  ███           ▒██L    ▀███▒██▌ ╫██▌       _▒██▌  ███▄         ╓███─     ▀███_
      |  ███,_________ ▒██L      ▀████▌ ╫██▌____,▄▄███▀   `▀███▄,___,▄████^       ^███▄
      |  ███████████▀^ ▒██L       "███▌ ╫██████████▀^       ^▀█████████▀^           ▀███φ
      |
      |                                                             -= customer-migrator =-
      |""".stripMargin

  private val parser = new OptionParser[Command]("logs-fetcher") {
    head(Console.BLUE + logo + Console.RESET)

    help("help")
      .text("Prints this usage text")

    note("")

    cmd("fetch")
      .text("Fetch logs from geth process")
      .action((_, _) => Fetch())
      .children(
        note(""),

        opt[CommunicationMode]("communicationMode")
          .valueName("<comm> [<url>]")
          .action {
            case (mode, config: Fetch) => config.copy(communicationMode = mode)
            case (_, config) => config
          }
          .text("The underlying connection method to geth"),

        opt[DefaultBlockParameter]("fromBlock")
          .valueName("<blockNumber> | earliest")
          .required()
          .action {
            case (from, config: Fetch) => config.copy(fromBlock = from)
            case (_, config) => config
          }
          .text("The first block to fetch logs from"),

        opt[DefaultBlockParameter]("toBlock")
          .valueName("<blockNumber> | latest")
          .required()
          .action {
            case (from, config: Fetch) => config.copy(toBlock = from)
            case (_, config) => config
          }
          .text("The last block to fetch logs from"),

        opt[Seq[String]]("topics")
          .valueName("<topic1>,<topic2>...")
          .action {
            case (topics, config: Fetch) => config.copy(topics = topics)
            case (_, config) => config
          }
          .text("The topics to filter on"),

        opt[File]("output")
          .valueName("<file>")
          .action {
            case (output, config: Fetch) => config.copy(output = output)
            case (_, config) => config
          }
          .text("The topics to filter on"),

        note("")
      )
  }

  private val maybeCommand = parser.parse(args, NoOp)

  maybeCommand
    .foreach {
      case NoOp =>
        parser.showUsage()

      case command: Fetch =>
        val service = command.communicationMode match {
          case CommunicationMode.IPC(path) => new IpcService(new UnixDomainSocket(path.toString()))
          case CommunicationMode.Http(url) => new HttpService(s"http://$url/")
        }
        val web3j = Web3j.build(service)
        val parser = new EthLogsParser(web3j)
        val filter = command.topics.foldLeft(new EthFilter(
          command.fromBlock,
          command.toBlock,
          (Nil: List[String]).asJava
        ))((filter, topic) => filter.addSingleTopic(topic))
        val events = web3j.ethGetLogs(filter).send()
          .getLogs
          .asScala
          .collect {
            case log: LogObject => log
          }
          .flatMap(parser.fromEthLog)
          .map(event => Json.toJson(event))
        val eventsJsArray = JsArray(events)
        val writer = new FileWriter(command.output, false)
        writer.write(eventsJsArray.toString())
        writer.close()
    }


}
