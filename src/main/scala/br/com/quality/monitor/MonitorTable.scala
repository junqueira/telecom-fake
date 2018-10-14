package br.com.quality.monitor

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import br.com.quality.MonitorFunc
import br.com.quality.monitor.MonitorBase.log
import br.com.quality.utils.{Data, SparkUtils}
import scala.concurrent.Future
import org.apache.spark.sql.functions.lit
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global


object MonitorTable {

        def execute(spark: SparkSession, dataBaseName: String): Future[DataFrame] = Future {
            try {
                val log: Logger = Logger.getLogger(MonitorTable.super.getClass)
                log.info(s"Start process asynchronously with base => " + dataBaseName)
                val spark2 = SparkUtils.getSparkSession(dataBaseName)
                //val tables = MonitorFunc.getTable(spark, dataBaseName)
                val tables = spark2.sql("select '" + dataBaseName.split(",")(0) + "' as no_tble, '" + dataBaseName.split(",")(1) + "' as no_base")
                var df = tables.transform(withTimestampInclusion())
                              .transform(withDateExit())
                              .transform(withDateExit())
                              .transform(withTableActive())
                              .transform(withNumVersion())
                              .transform(withSizeTable())
                              .transform(withCountLineTable())
                              .transform(withIndTableNew())
                              .transform(withDateRef())
                log.info(s"Final process => " + dataBaseName)
                df
            }
            catch {
                case _: Throwable => null
            }
        }

        def withTimestampInclusion ()(df: DataFrame): DataFrame = {
            df.withColumn("ts_incs", lit(Data.getTimestamp()))
        }

        def withDateExit ()(df: DataFrame): DataFrame = {
            df.withColumn("dt_said", lit(Data.dateToString()))
        }

        def withTableActive ()(df: DataFrame): DataFrame = {
            df.withColumn("in_atvo", lit("1"))
        }

        def withNumVersion ()(df: DataFrame): DataFrame = {
            df.withColumn("nu_vrso", lit("1"))
        }

        def withSizeTable ()(df: DataFrame): DataFrame = {
            df.withColumn("qt_tmnh_tble",
              MonitorFunc.getSizeTableUDF(col("no_base"), col("no_tble")))
        }

        def withCountLineTable ()(df: DataFrame): DataFrame = {
            df.withColumn("qt_lnha_tble", MonitorFunc.getCountLineTableUDF(
                col("no_base"), col("no_tble"), col("qt_tmnh_tble")))
        }

        def withIndTableNew ()(df: DataFrame): DataFrame = {
            df.withColumn("in_tble_nova_dia_atul", lit("1"))
        }

        def withDateRef ()(df: DataFrame): DataFrame = {
            df.withColumn("dt_foto", lit(Data.dateToString()))
        }
}