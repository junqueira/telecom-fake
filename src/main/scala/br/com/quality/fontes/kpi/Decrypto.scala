package br.com.fatura.crypto

import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import br.com.quality.utils.Util


object Decrypto {

		val log: Logger = Logger.getLogger(Decrypto.getClass)

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
				fileOutput.write.mode(SaveMode.Overwrite).csv(hdfs_master + "decrypt")
		}


}