package br.com.quality.decrypto

import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import br.com.quality.utils.Util
import br.com.quality.utils.Storage
import br.com.quality.utils.Storage.conf
import com.typesafe.config.ConfigFactory


object Decrypto {

		val log: Logger = Logger.getLogger(Decrypto.getClass)
    val conf = ConfigFactory.load()
    val s3BucketName = conf.getString("main.s3BucketName")

		def execute(spark: SparkSession, config: DataFrame, pathFile: String = "", pathDict: String = ""): Unit = {
				import spark.implicits._
				val hdfs_master = "decrypt/files_out/"
			  val fileDF = Util.getFile(spark, pathFile)
			  val dictDF = Util.getFile(spark, pathDict, false)

				val lookupMap = dictDF.filter("_c2 is not null")
															.map{case Row(category:String,field_values:String,ratio:String) =>
																		((category,field_values),ratio)}.collect().toMap
				val bc_lookupMap = spark.sparkContext.broadcast(lookupMap)

				val column = config.select("column").first().mkString.split("\\s")

				val fileOutput = column.foldLeft(fileDF) { (df, colName) =>
							df.transform(DecryptoFunc.withCrypto(bc_lookupMap,  colName))
				}

				fileDF.show(false)
				dictDF.show(false)
				fileOutput.show(false)
				//fileOutput.write.mode(SaveMode.Overwrite).csv(hdfs_master + "decrypt")
        Storage.getBaseS3(s3BucketName, "arquivo_referencia.txt")
		}


}