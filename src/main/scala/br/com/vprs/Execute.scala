package br.com.quality

import br.com.quality.utils.SparkUtils
import org.apache.log4j._
import br.com.quality.utils.{Storage, Util}
import br.com.quality.decrypto.{Decrypto, DecryptoFunc}
import br.com.quality.utils.Storage.conf


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

//      if(fonte.isEmpty || dtfoto.isEmpty || args.contains("-h")) showHelp()

      val spark = SparkUtils.getSparkSession(fonte)

      log.info(s"**********************************************************************************")
      log.info(s"*** Fonte: $fonte")
      log.info(s"*** DtFoto: $dtfoto")
      log.info(s"**********************************************************************************")

      try {
          fonte match {

              case "decrypto" =>
                  log.info(s"Decryption data generation")
                  val s3BucketName = conf.getString("main.s3BucketName")
                  val s3FileName = conf.getString("main.s3FileName")

//                  val config = Util.getConfig(spark, base)
//                  if (config.count == 1) {
//                      Decrypto.execute(spark, config, pathFile, pathDict, pathDest)
//                  } else {
//                      println("not exist base in file config, options are => ")
//                      config.show(false)
//                  }
                  val file = Storage.getBaseS3(s3BucketName, s3FileName)
//                  println(file)
                  val df = DecryptoFunc.fileToDF(spark, file)
                  df.show(false)

              case _ =>
                  log.error(s"Source not found!")
                  sys.exit(134)
          }
      }catch {
          case e : Exception =>
              log.error(s"Error in proccess: "+e.getMessage)
              e.printStackTrace()
      }

      log.info(s"End proccess quality")

  }

  def showHelp(): Unit = {
      log.info(s" ==> HELP")
      log.info(s"======================================================================================== ")
      log.info("usage    : spark-submit quality-assembly-1.0.jar --fonte $fonte --dtfoto $dtfoto")
      log.info("$fonte   : font name, like (decrypto, etc...)")
      log.info("$dtfoto  : dtfoto - yyyyMMdd")
      log.info(s"========================================================================================")
      sys.exit(134)
  }

}
