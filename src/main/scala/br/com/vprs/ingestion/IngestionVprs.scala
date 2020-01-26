package br.com.vprs.ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.{SparkSession}


object IngestionVprs {

    def execute(spark: SparkSession): Unit = {
        val log = Logger.getLogger(IngestionVprs.getClass)
	    log.info(s"Init process vrps table dpi")
        val vprsdf = spark.read.option("sep","|")
            .csv("/main_stage/vPRS/sig/in/AO*2020012407*.gz")
        vprsdf.show(20, false)

        val newvprsdf = vprsdf.selectExpr("_c0 as probeid", 
"_c1 as starttime",
"_c2 as starttimemsel",
"_c3 as endtime",
"_c4 as endtimemsel",
"_c5 as protocolid",
"_c6 as subprotocolid",
"_c7 as msisdn",
"_c8 as imsi",
"_c9 as imei",
"_c10 as localipv4",
"_c11 as remoteipv4",
"_c12 as localport",
"_c13 as remoteport",
"_c14 as l4protocol",
"_c15 as apn",
"_c16 as gnipaddress",
"_c17 as snipaddress",
"_c18 as rat",
"_c19 as rai",
"_c20 as cgi",
"_c21 as sai",
"_c22 as tai",
"_c23 as ecgi",
"_c24 as inputoctets",
"_c25 as outputoctets",
"_c26 as sessionid",
"_c27 as protocolname",
"_c28 as subprotocolname");

val refinalvprsdf = newvprsdf.coalesce(100)

refinalvprsdf.write.mode(SaveMode.Overwrite).saveAsTable("p_desenvolvimento_db.tt_vprs");

    }
}

