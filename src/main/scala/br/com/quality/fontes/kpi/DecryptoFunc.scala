package br.com.fatura.crypto
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._


object DecryptoFunc {
    val log: Logger = Logger.getLogger(DecryptoFunc.getClass)

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