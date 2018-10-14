package br.com.quality

import java.io.{PrintWriter, StringWriter}
import br.com.quality.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, udf}
import br.com.quality.utils.HiveUtils
import org.apache.log4j.Logger
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


object MonitorFunc {
    val log: Logger = Logger.getLogger(MonitorFunc.getClass)

    def getBases(spark: SparkSession, nameBase: String = ""): DataFrame = {
        var bases = spark.sql("show databases")
        if (!nameBase.isEmpty()) {
            val filterDB = s"databaseName like '%$nameBase%'"
            bases = bases.filter(filterDB)
        }
        bases.withColumnRenamed("databaseName", "no_base")
    }

    def getTable(spark: SparkSession, dbTable: String): DataFrame = {
        spark.sql("select '" + dbTable.split(",")(0) + "' as no_tble, '" + dbTable.split(",")(1) + "' as no_base")
    }

    def getTables(spark: SparkSession, nameBase: String = "p_bigd_db"): DataFrame = {
        spark.sql(s"use $nameBase")
        val tables = spark.sql("show tables").select("database", "tableName")
        tables.withColumnRenamed("tableName", "no_tble")
            .withColumnRenamed("database", "no_base")
            .withColumn("no_base", lit(nameBase))
    }

    def getAllTables(spark: SparkSession): List[String] = {
//        val bases = getBases(spark, "p_desenvolvimento_db")
        val bases = getBases(spark)
        var allTables = List[String]()
        val list_base = bases.select("no_base").rdd.map(base => base(0)).collect
        list_base.foreach(base => {
            if (!base.toString.equals("p_bigd_db")) {
                val tables = getTables(spark, base.toString).collect.foreach(table => {
//                    if (allTables.size < 1000) {
                        allTables ::= table(1).toString + "," + table(0).toString
//                    }
                })
            }
        })
        allTables
    }

    val getSizeBaseUDF = udf[String, String](getSizeBase)
    def getSizeBase(nameBase: String): String = {
        getSizeTable(nameBase)
    }

    def truncateAt(n: Double, p: Int=2): Double = {
        val s = math pow (10, p); (math floor n * s) / s
    }

    val getCountLineBaseUDF = udf[String, String, String, String](getCountLineBase)
    def getCountLineBase(base: String, table: String, spaceFisic: String = "0"): String = {
        getCountLineTable(base, table, spaceFisic)
    }

    val getCountLineTableUDF = udf[String, String, String, String](getCountLineTable)
    def getCountLineTable(base: String, table: String, spaceFisic: String = "0"): String = {
        // big-table ~>1Tera not work in spark => GC overhead limit exceeded - Seq[String]
        try {
            if (spaceFisic.toFloat <= 10000) {
                val sqlCount = s"select count(*) from " + base + "." + table
                if (spaceFisic.toFloat >= 100) {
                    val bl = HiveUtils.beelineExec(sqlCount).trim
                    println("retorno => " + bl)
                    bl.toString
                } else {
                    val spark = SparkUtils.getSparkSession(table)
                    println(s"count => $base.$table")
                    spark.sql(sqlCount).first()(0).toString
                }
            }
            "0.0"
        }
        catch {
            case e : Exception =>
                val sw = new StringWriter
                e.printStackTrace(new PrintWriter(sw))
                sw.toString
        }
    }

    val getSizeTableUDF = udf[String, String, String](getSizeTable)
    def getSizeTable(nameBase: String = "p_bigd_db", nameTable: String = ""): String = {
        // "KB", "MB", """GB""", "TB", "PB", "EB", "ZB", "YB"  -> return in GB
        import sys.process._
        var cmdSizeBase: String = ""
        if (nameBase == "" && nameTable == "") {
            "0.0"
        }
        if (nameBase != "") {
            cmdSizeBase = s"hdfs dfs -du -s -h /apps/hive/warehouse/$nameBase.db/"
        } else {
            "0.0"
        }
        if (nameTable != "") {
            cmdSizeBase = cmdSizeBase.concat(nameTable)
        }
        try {
            val resultSize = Future(cmdSizeBase.!!)
            val resultSizeTotal = Await.result(resultSize, Duration.Inf)
            val sizeBase = resultSizeTotal.toString.split("  ")(0).split(' ')
            var size: Double = 0
            if (sizeBase.last == "K") {
                size = sizeBase.head.toFloat / math.pow(1000.0, 2)
            } else if (sizeBase.last == "M") {
                size = sizeBase.head.toFloat / 1000.0
            } else if (sizeBase.last == "G") {
                size = sizeBase.head.toFloat
            } else if (sizeBase.last == "T") {
                size = sizeBase.head.toFloat * (1000.0)
            } else if (sizeBase.last == "P") {
                size = sizeBase.head.toFloat * (1000.0 * 2)
            } else if (sizeBase.last == "E") {
                size = sizeBase.head.toFloat * (1000.0 * 3)
            } else if (sizeBase.last == "Z") {
                size = sizeBase.head.toFloat * (1000.0 * 4)
            } else if (sizeBase.last == "Y") {
                size = sizeBase.head.toFloat * (1000.0 * 5)
            }
            truncateAt(size).toString
        }
        catch {
            case _: Throwable => "0.0"
//                case e : Exception =>
//                    e.printStackTrace()
//                    "0.0"
            }
        }

}
