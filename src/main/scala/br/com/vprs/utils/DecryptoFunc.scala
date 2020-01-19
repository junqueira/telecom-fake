package br.com.quality.decrypto

import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


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
    
    
    def getFile(spark: SparkSession, pathFile: String = "", separator: String, header: String ="true") : DataFrame = {
        val fileDF = spark.read.csv(pathFile)
        if (header=="true") {
            val schemaFile = fileDF.first().toString()
            val fields = schemaFile.split(separator).map(fieldName => StructField(fieldName, StringType, nullable = true))
            val schema = StructType(fields)
            var dfWithSchema = spark.read.option("header", header).option("delimiter", separator).schema(schema).csv(file)
              .repartition(300)
        } else {
            fileDF
        }
    }
    
    def saveDfToCsv(df: DataFrame, tsvOutput: String = "asdasdf444", sep: String = ",", header: Boolean = false): Unit
    = {
        val tmpParquetDir = "Posts.tmp.parquet"
        df.repartition(1).write
        .format("com.databricks.spark.csv")
//          .format("parquet")
          .option("header", header.toString)
          .option("delimiter", sep)
          .option("inferSchema", "true")
          .option("mode", "FAILFAST")
          .option("path", tmpParquetDir)
          .save()
        
        val dir = new File(tmpParquetDir)
        val newFileRgex = tmpParquetDir + File.separatorChar + ".part-00000.*.csv"
        val tmpTsfFile = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString
        (new File(tmpTsvFile)).renameTo(new File(tsvOutput))
        
        dir.listFiles.foreach( f => f.delete )
        dir.delete
    }
    
//    >>> # This is a better way to change the schema
//    >>> df_rows = sqlContext.createDataFrame(df_rows.rdd, df_table.schema)
    def dfToFile(spark: SparkSession, df: DataFrame, pathFile: String) : Unit = {
//        val separator = getConfig(configDF, "separator")
        val separator = ";"
        val tsvWithHeaderOptions: Map[String, String] = Map(
            ("delimiter", separator.toString),
            ("header", "true"),
            ("compression", "None"),
            ("inferSchema", "true"),
            ("mode", "FAILFAST"))
    
        val schemaFile = df.first().toString()
        val fields = schemaFile.split(separator).map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)
        
        val fileTempPart = pathFile.concat(".tmp")
        val dfTest = spark.sqlContext.createDataFrame(df.rdd, df.schema)
        dfTest.coalesce(1).write
//          .format("com.databricks.spark.csv")
          .format("orc")
           .option("quote","")
          .mode(SaveMode.Overwrite)
          .options(tsvWithHeaderOptions)
//          .schema(schema)
          .csv(fileTempPart)
//          .parquet(fileTempPart)
//          .save(fileTempPart)
        
        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
        val fileTemp = fs.globStatus(new Path(fileTempPart+"/part*"))(0).getPath().getName()
        
        val pathFileIn = fileTempPart.concat("/" + fileTemp)
        val pathFileOut = pathFile.toString.concat(".des")
        fs.rename(new Path(pathFileIn), new Path(pathFileOut))
        fs.delete(new Path(fileTempPart))
        println("file save in => " + pathFileOut)
    }
    
    def fileToDF(spark: SparkSession, file: String): DataFrame = {
        import spark.implicits._
        val list = file.split("\n").map(_.split(",")) //.transpose
        val rdd = spark.sparkContext.parallelize(list).map(row => Decrypto(row(0), row(1), row(2)))
        rdd.toDF
    }
}