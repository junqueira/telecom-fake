package br.com.fatura.crypto
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types._


object DecryptoFunc {
    val log: Logger = Logger.getLogger(DecryptoFunc.getClass)

    def getFile(spark: SparkSession, file: String = "", schema: Boolean = true): DataFrame = {
        if (schema) {
            val schemaString = spark.read.csv(file).first()(0).toString()
            val fields = schemaString.split("\t").map(fieldName => StructField(fieldName, StringType, nullable = true))
            val schema = StructType(fields)
            val dfWithSchema = spark.read.option("header", "true").option("delimiter", "\t").schema(schema).csv(file)
            dfWithSchema
        } else {
            spark.read.csv(file)
        }
    }

    def getConfig(spark: SparkSession, base: String): DataFrame = {
        val config = "dictionary.json"
        val df = spark.read.json(spark.sparkContext.wholeTextFiles(config).values)
        if (base != "") {
            val config = df.filter("name='" + base.toString + "'")
            if (config.count  == 0 ) {
                println("Not found base -> " + base + " the possibilities are")
                df
            } else {
                config
            }
        } else {
            df
        }
    }

}