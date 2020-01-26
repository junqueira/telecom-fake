package br.com.vprs.utils

import java.io.{ByteArrayOutputStream, InputStream}

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Util {

    def truncateAt(n: Double, p: Int=2): Double = {
        val s = math pow (10, p); (math floor n * s) / s
    }

    def toBytes(in: InputStream): Array[Byte] = {
        try {
            val out = new ByteArrayOutputStream()

            var length = 0
            var buf = new Array[Byte](1024 * 8)
            while(length != -1){
              length = in.read(buf)
              if(length > 0){
                out.write(buf, 0, length)
              }
            }
            out.toByteArray
        } finally {
            in.close()
        }
    }

    def getFile(spark: SparkSession, file: String = "", schema: Boolean = true): DataFrame = {
        if (schema) {
            val schemaString = spark.read.csv(file).first()(0).toString()
            val fields = schemaString.split("\t").map(fieldName => 
                StructField(fieldName, StringType, nullable = true)
            )
            val schema = StructType(fields)
            val dfWithSchema = spark.read.option("header", "true")
                .option("delimiter", "\t").schema(schema).csv(file)
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
    
    def dataFrameToFile(spark: SparkSession, df: DataFrame, pathFile: String, separator: String="|"): Unit = {
        val tsvWithHeaderOptions: Map[String, String] = Map(
          ("delimiter", separator.toString),
          ("header", "true"),
          ("compression", "None"))

        //val tempFile = pathFile + ".tmp"
        df.coalesce(300).write
            .mode(SaveMode.Overwrite)
            .options(tsvWithHeaderOptions)
            .format("csv")
            //.save(tempFile)
            .save(pathFile)

//        val fs = FileSystem.get( spark.sparkContext.hadoopConfiguration )
//        val oldFile = fs.globStatus(new Path(tempFile+"/part*"))(0).getPath().getName()
//        fs.rename(new Path(tempFile +"/"+ oldFile), new Path(pathFile))
//        fs.delete(new Path(tempFile))
        println("file save in => " + pathFile)
    }

//    def parellelize(spark): Unit = {
//        sc.parallelize(dfQueries.filter($"num_seq" >= numIni and $"num_seq" <= numFim).rdd.map(r => r(0)).collect.toList)
//        .map(a => executarHql(a.toString,"Qualidade")).
//        collect.foreach( a => { if(a contains "Erro: "){ accLog+=a }else{ acc+=a.filterNot((x: Char) => x.isWhitespace) } })
//
//    }

}
