package br.com.vrps.IngestionVprs

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.{SparkSession}


object IngestionVprs {

    def execute(spark: SparkSession, dtfoto: String): Unit = {
        val log = Logger.getLogger(IngestionVprs.getClass)
	    log.info(s"Init process vrps table kpi")

    }
}