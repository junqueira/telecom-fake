package br.com.qualidade.fontes.erb


object KpiIntegrity extends Enumeration {

    protected case class Val(hql: String) extends super.Val {}

    val kpiQtdTotalKpiLocal = Val(
        s"""
           |select
           |    count(distinct concat(science.sg_uf, science.sg_erb) ) as qt_total_erb
           |from
           |    p_bigd_db.tbgdt_tgt_apo_science science
           |where
           |    science.dt_foto = "{{dt_foto}}"
    """.stripMargin
    )

    val kpiQtdTotalSiteLocal = Val(
        s"""
           |select
           |    count(distinct concat(science.sg_uf, science.sig_site) ) as qt_total_site
           |from
           |    p_bigd_db.tbgdt_tgt_apo_science science
           |where
           |    science.dt_foto = "{{dt_foto}}"
    """.stripMargin
    )

    val kpiQtdDuplicadosKpiLocal = Val(
        s"""
           | select
           |     count(*) as qt_duplicidades_erb
           | from (
           |    select
           |           science.sg_uf, science.sg_erb
           |    from
           |           p_bigd_db.tbgdt_tgt_apo_science science
           |    where
           |           science.dt_foto = "{{dt_foto}}"
           |    group by
           |           science.sg_uf, science.sg_erb
           |    having count(*) > 1
           | ) a
    """.stripMargin
    )

    val kpiQtdDuplicadosSiteLocal = Val(
        s"""
           | select
           |     count(*) as qt_duplicidades_site
           | from (
           |    select
           |           science.sg_uf, science.sig_site
           |    from
           |           p_bigd_db.tbgdt_tgt_apo_science science
           |    where
           |           science.dt_foto = "{{dt_foto}}"
           |    group by
           |           science.sg_uf, science.sig_site
           |    having count(*) > 1
           | ) a
    """.stripMargin
    )

    val tableFinalKpiLocal = Val(
        s"""
           |insert overwrite table p_desenvolvimento_db.tbgd_tdq_work_qlto_dia_cgi_kpi
           |partition(dt_foto)
           |    select
           |            cd_sem,
           |            qt_total_erb,
           |            qt_total_cgi,
           |            qt_total_site,
           |            qt_duplicidades_erb,
           |            qt_duplicidades_site,
           |            qt_muda_posicao,
           |            qt_ibge_null,
           |            qt_lati_long_null,
           |            qt_erb_fora_brasil,
           |            dt_foto
           |      from
           |            tempViewERBKpi
    """.stripMargin
    )

}
