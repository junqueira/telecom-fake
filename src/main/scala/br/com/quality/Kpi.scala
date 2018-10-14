package br.com.quality
//import br.com.quality.fontes.kpi.{KpiIntegrity}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.log4j._

object Kpi {

    def execute(spark: SparkSession, dtfoto: String): Unit = {
//        val log = Logger.getLogger(Mcdr.getClass)
//
//        val kpiQtdTotalKpiLocal = spark.sql(KpiIntegrity.kpiQtdTotalKpiLocal.hql.replace("{{dt_foto}}",dtfoto)).withColumn("dt_foto", lit(dtfoto))
//        val kpiQtdTotalSiteLocal = spark.sql(KpiIntegrity.kpiQtdTotalSiteLocal.hql.replace("{{dt_foto}}",dtfoto)).withColumn("dt_foto", lit(dtfoto))
//        val kpiQtdDuplicadosKpiLocal = spark.sql(KpiIntegrity.kpiQtdDuplicadosKpiLocal.hql.replace("{{dt_foto}}",dtfoto)).withColumn("dt_foto", lit(dtfoto))
//        val kpiQtdDuplicadosSiteLocal = spark.sql(KpiIntegrity.kpiQtdDuplicadosSiteLocal.hql.replace("{{dt_foto}}",dtfoto)).withColumn("dt_foto", lit(dtfoto))
//
//        val finalDF =
//            kpiQtdTotalKpiLocal
//                .join(kpiQtdTotalKpiLocal, "dt_foto")
//                .join(kpiQtdTotalSiteLocal, "dt_foto")
//                .join(kpiQtdDuplicadosSiteLocal, "dt_foto")
//
//        val reorderedColumnNames =
//            Array("cd_sem", "qt_total_erb", "qt_total_cgi", "qt_total_site", "qt_duplicidades_erb", "qt_duplicidades_site", "qt_muda_posicao", "qt_ibge_null", "qt_lati_long_null", "qt_erb_fora_brasil", "dt_foto")
//
//        finalDF.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
//             .createOrReplaceTempView("tempViewERBKpi")
//
//        spark.sql(KpiIntegrity.tableFinalKpiLocal.hql)
//
//        //Execução qualidade tabela KPI's CEP - DIA
//        log.info(s"Inicio qualidade tabela KPI - CEP/DIA")
//        spark.sql(KpiCepDia.dropTableTmpCepDesvio.hql)
    }

}
