package com.aws.analytics.conf

case class Config(

                   checkpointDir: String ="",
                   checkpointInterval:String ="60",
                   hudiPath:String="",
                   hudiTableName:String="",
                   rowsPerSecond:String="100"

                 )


object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config](programName) {
      head(programName, "1.0")
      opt[String]('c', "checkpointDir").required().action((x, config) => config.copy(checkpointDir = x)).text("checkpoint dir")
      opt[String]('l', "checkpointInterval").optional().action((x, config) => config.copy(checkpointInterval = x)).text("checkpoint interval: default 60 seconds")

      programName match {
        case "DataGen2Hudi" =>
          opt[String]('p', "hudiPath").required().action((x, config) => config.copy(hudiPath = x)).text("hudi path: eg. s3://xxx/xxx/")
          opt[String]('t', "hudiTableName").required().action((x, config) => config.copy(hudiTableName = x)).text("hudi table name")
          opt[String]('r', "rowsPerSecond").optional().action((x, config) => config.copy(rowsPerSecond = x)).text("ddatagen rows-per-second, default:100")

        case _ =>

      }


    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        //        println("cannot parse args")
        System.exit(-1)
        null
      }
    }

  }

}
