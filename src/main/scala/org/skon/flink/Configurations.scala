package org.skon.flink

case class Configurations(maxParallelism: Int = 4, parallelismForTimestamp: Int = 4)

object Configurations {

  private val parser = new scopt.OptionParser[Configurations]("Parallelism with Global Window") {
    head("scopt", "3.x")

    opt[Int]('m', "max-parallelism")
      .action((x, c) => c.copy(maxParallelism = x))
      .text("maximum number of parallelism")
      .validate(x => {
        if (x > 0) success
        else failure("Value of <max-parallelism> should be greater than 0")
      })
      .text("Maximum number of parallelism")

    opt[Int]('t', "parallelism-for-timestamp")
      .action((x, c) => c.copy(parallelismForTimestamp = x))
      .text("maximum number of parallelism of timestamp")
      .validate(x => {
        if (x > 0) success
        else failure("Value of <parallelism-for-timestamp> should be greater than 0")
      })
      .text("Number of parallelism for assigning timestamp")

    help("help").text("prints this usage text")
  }


  def get(args: Array[String]): Configurations = {
    parser.parse(args, Configurations()) match {
      case Some(configurations) =>
        configurations
      case _ => null
    }
  }
}