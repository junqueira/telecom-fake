package br.com.vprs

import br.com.vprs.utils.SparkUtils
import org.apache.log4j._
import br.com.vprs.utils.{Util}
import br.com.vprs.ingestion.IngestionVprs


object Execute {

  val log: Logger = Logger.getLogger(Execute.getClass)

  def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      Logger.getLogger("hive").setLevel(Level.OFF)

      log.info(s"Initialize proccess")

      try {
          args.grouped(2).map(x => x(0).replace("--","") -> x(1)).toMap
      } catch {
          case _ : Exception => showHelp()
      }

      val params = args.grouped(2).map(x => x(0).replace("--","") -> x(1)).toMap
      val fonte = params.getOrElse("fonte", "").toLowerCase
      //val dtfoto = params.getOrElse("dtfoto", "").toLowerCase

      //if(fonte.isEmpty || dtfoto.isEmpty || args.contains("-h")) showHelp()

      val spark = SparkUtils.getSparkSession(fonte)

      log.info(s"**********************************************************************************")
      log.info(s"*** Fonte: $fonte")
      //log.info(s"*** DtFoto: $dtfoto")
      log.info(s"**********************************************************************************")

      try {
          fonte match {

              case "vprs" =>
                  log.info(s"ingestion vprs")
                  //val config = Util.getConfig(spark, base)
                  IngestionVprs.execute(spark)

              case _ =>
                  log.error(s"Source not found!")
                  sys.exit(134)
          }
      }catch {
          case e : Exception =>
              log.error(s"Error in proccess: "+e.getMessage)
              e.printStackTrace()
      }

      log.info(s"End proccess vprs")

  }

  def showHelp(): Unit = {
      log.info(s" ==> HELP")
      log.info(s"======================================================================================== ")
      log.info("usage    : spark-submit vprs-assembly-1.0.jar --fonte $fonte --dtfoto $dtfoto")
      log.info("$fonte   : font name, like (decrypto, etc...)")
      log.info("$dtfoto  : dtfoto - yyyyMMdd")
      log.info(s"========================================================================================")
      sys.exit(134)
  }

}
