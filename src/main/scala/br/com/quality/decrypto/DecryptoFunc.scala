package br.com.quality.decrypto

import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._


object DecryptoFunc {
    val log: Logger = Logger.getLogger(DecryptoFunc.getClass)

    case class Decrypto(field: String, key: String, value: String)

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

    def fileToDF(spark: SparkSession, file: String): DataFrame = {
        import spark.implicits._
        val list = file.split("\n").map(_.split(",")) //.transpose
        val rdd = spark.sparkContext.parallelize(list).map(row => Decrypto(row(0), row(1), row(2)))
        rdd.toDF
    }
}