package br.com.quality.kpi

import br.com.quality.kpi.hql.KpiIntegrity
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.{SparkSession}


object Kpi {

    def execute(spark: SparkSession, dtfoto: String): Unit = {
        val log = Logger.getLogger(Kpi.getClass)
				log.info(s"Init process quality table kpi")

        val kpiQtdTotalKpiLocal = spark.sql(KpiIntegrity.kpiQtdTotalKpiLocal.hql.replace("{{dt_foto}}",dtfoto)).withColumn("dt_foto", lit(dtfoto))
        val kpiQtdDuplicadosKpiLocal = spark.sql(KpiIntegrity.kpiQtdDuplicadosKpiLocal.hql.replace("{{dt_foto}}",dtfoto)).withColumn("dt_foto", lit(dtfoto))

        val finalDF =
            kpiQtdTotalKpiLocal
                .join(kpiQtdTotalKpiLocal, "dt_foto")

        val reorderedColumnNames =
            Array("cd_week", "qt_total", "qt_duplicity", "qt_change_position", "qt_null", "bigint", "qt_lati_long_null", "qt_outside_region", "dt_foto", "dt_foto")

				finalDF.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
             .createOrReplaceTempView("tempViewERBKpi")

        spark.sql(KpiIntegrity.tableFinalKpiLocal.hql)
    }

}
