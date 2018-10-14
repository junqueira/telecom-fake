package br.com.quality.monitor.hql


object KpiDataBase extends Enumeration {

    protected case class Val(hql: String) extends super.Val {}

    val cdSemana = Val(
        s"""
           |select
           |    cast(from_unixtime(unix_timestamp('{{dt_foto}}','yyyy-MM-dd'),'u')as int) as cd_smna
        """.stripMargin
    )

    val kpiQtdTotalKpiLocal = Val(
        s"""
           |select
           |    count(distinct concat(science.sg_uf, science.sg_erb) ) as qt_total
           |from
           |    prod.qlto_dia_kpi science
           |where
           |    science.dt_foto = "{{dt_foto}}"
        """.stripMargin
    )

    val kpiQtdDuplicadosKpiLocal = Val(
        s"""
           | select
           |     count(*) as qt_duplicity
           | from (
           |    select
           |           science.sg_uf, science.sg_erb
           |    from
           |           prod.qlto_dia_kpi science
           |    where
           |           science.dt_foto = "{{dt_foto}}"
           |    group by
           |           science.sg_uf, science.sg_erb
           |    having count(*) > 1
           | ) a
        """.stripMargin
    )

    val tableFinalKpiLocal = Val(
        s"""
           |insert overwrite table desenv.qlto_dia_kpi
           |partition(dt_foto)
           |    select
           |        cd_week,
           |        qt_total,
           |        qt_duplicity,
           |        qt_change_position,
           |        qt_null` bigint,
           |        qt_lati_long_null,
           |        qt_outside_region
           |    from
           |        tempViewKpi
          """.stripMargin
    )
}