package br.com.quality

import br.com.quality.utils.SparkUtils
import org.apache.log4j._


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
      val dtfoto = params.getOrElse("dtfoto", "").toLowerCase

      if(fonte.isEmpty || dtfoto.isEmpty || args.contains("-h")) showHelp()

      val spark = SparkUtils.getSparkSession(fonte)

      log.info(s"**********************************************************************************")
      log.info(s"*** Fonte: $fonte")
      log.info(s"*** DtFoto: $dtfoto")
      log.info(s"**********************************************************************************")

      try {
          fonte match {

            case _ =>
                log.error(s"Fonte não encontrada!")
                sys.exit(134)
          }
      }catch {
          case e : Exception =>
              log.error(s"Erro no processo: "+e.getMessage)
              e.printStackTrace()
      }

      log.info(s"Fim processo de qualidade")

  }

  def showHelp(): Unit = {
      log.info(s" ==> HELP")
      log.info(s"======================================================================================== ")
      log.info("usage    : spark-submit quality-assembly-1.0.jar --fonte $fonte --dtfoto $dtfoto")
      log.info("$fonte   : font name, like (, etc...)")
      log.info("$dtfoto  : dtfoto - yyyyMMdd")
      log.info(s"========================================================================================")
      sys.exit(134)
  }

}
