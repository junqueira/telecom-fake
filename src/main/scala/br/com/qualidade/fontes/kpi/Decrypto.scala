package br.com.fatura.crypto
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object Decrypto {

		val log: Logger = Logger.getLogger(Decrypto.getClass)

		def execute(spark: SparkSession, config: DataFrame, pathFile: String = "", pathDict: String = ""): Unit = {
				import spark.implicits._
				val hdfs_master = "decrypt_iptv/files_out/"
			  val fileDF = DecryptoFunc.getFile(spark, pathFile)
			  val dictDF = DecryptoFunc.getFile(spark, pathDict, false)

				val lookupMap = dictDF.filter("_c2 is not null")
															.map{case Row(category:String,field_values:String,ratio:String) =>
																		((category,field_values),ratio)}.collect().toMap
				val bc_lookupMap = spark.sparkContext.broadcast(lookupMap)

				val column = config.select("column").first().mkString.split("\\s")

				val fileOutput = column.foldLeft(fileDF) { (df, colName) =>
							df.transform(withCrypto(bc_lookupMap,  colName))
				}

				fileDF.show(false)
				dictDF.show(false)
				fileOutput.show(false)
				fileOutput.write.mode(SaveMode.Overwrite).csv(hdfs_master + "decrypt_iptv")
		}

		def withCrypto(dictDF: Broadcast[Map[(String,String), String]], field: String)(fileDF: DataFrame): DataFrame = {
				def getLookUp(column: String, field: String): String = {
					try {
							dictDF.value((column, field))
					}
					catch {
							case _: Throwable => "null"
					}
				}
				val lookUpUdf = udf[String, String, String](getLookUp)
				fileDF.withColumn(field.concat("_HASH"), col(field))
							.withColumn(field, lookUpUdf(lit(field), col(field)))
		}
}