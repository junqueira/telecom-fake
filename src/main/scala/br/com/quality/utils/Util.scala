package br.com.quality.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Util {

    def truncateAt(n: Double, p: Int=2): Double = {
        val s = math pow (10, p); (math floor n * s) / s
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

    case class Location(lat: Double, lon: Double)
    trait DistanceCalcular {
        def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int
    }

    class DistanceCalculatorImpl extends DistanceCalcular {
        private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
        override def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int = {
          val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
          val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
          val sinLat = Math.sin(latDistance / 2)
          val sinLng = Math.sin(lngDistance / 2)
          val a = sinLat * sinLat +
            (Math.cos(Math.toRadians(userLocation.lat)) *
              Math.cos(Math.toRadians(warehouseLocation.lat)) *
              sinLng * sinLng)
          val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
          (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
        }
    }
    new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(10, 20), Location(40, 20))

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
