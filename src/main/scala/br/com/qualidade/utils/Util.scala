package br.com.fatura.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{DataFrame, SparkSession}


object Util {

    def truncateAt(n: Double, p: Int=2): Double = {
        val s = math pow (10, p); (math floor n * s) / s
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
