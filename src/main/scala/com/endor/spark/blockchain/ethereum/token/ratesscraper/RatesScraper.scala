package com.endor.spark.blockchain.ethereum.token.ratesscraper

import java.io.{File, FileWriter}
import java.time.Instant

import com.endor.spark.blockchain.ethereum.token.TokenRatesScraper
import play.api.libs.json._
import scopt.OptionParser



sealed trait Command

object Command {
  final case class Fetch(symbol: String = "", from: Instant = Instant.now(),
                         to: Instant = Instant.now(), output: File = new File("logs.out")) extends Command

  case object NoOp extends Command

  implicit val instantRead: scopt.Read[Instant] = scopt.Read.reads[Instant](Instant.parse)
}

object RatesScraper extends App {
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

  private val parser = new OptionParser[Command]("rates-scraper") {
    head(Console.BLUE + logo + Console.RESET)

    help("help")
      .text("Prints this usage text")

    note("")

    cmd("fetch")
      .text("Fetch rates from coinmarketcap")
      .action((_, _) => Fetch())
      .children(
        note(""),

        opt[String]('s', "symbol")
          .valueName("<tokenSymbol>")
          .action {
            case (symbol, config: Fetch) => config.copy(symbol = symbol)
            case (_, config) => config
          }
          .text("The symbol to fetch rates for"),

        opt[Instant]('f', "from")
          .valueName("<fromInstant> (format: yyyy-MM-ddTHH:mm:ssZ)")
          .required()
          .action {
            case (from, config: Fetch) => config.copy(from = from)
            case (_, config) => config
          }
          .text("The first date to fetch rates"),

        opt[Instant]('t', "to")
          .valueName("<toInstant> (format: yyyy-MM-ddTHH:mm:ssZ)")
          .required()
          .action {
            case (to, config: Fetch) => config.copy(to = to)
            case (_, config) => config
          }
          .text("The to date to fetch rates"),

        opt[File]('o', "output")
          .valueName("<file>")
          .action {
            case (output, config: Fetch) => config.copy(output = output)
            case (_, config) => config
          }
          .text("Where to save the output"),

        note("")
      )
  }

  private val maybeCommand = parser.parse(args, NoOp)

  maybeCommand
    .foreach {
      case NoOp =>
        parser.showUsage()

      case command: Fetch =>
        val scraper = new TokenRatesScraper()
        val rates = scraper.scrapeToken(command.symbol, command.from, command.to)
        val writer = new FileWriter(command.output, false)
        writer.write(Json.toJson(rates).toString())
        writer.close()
    }


}
