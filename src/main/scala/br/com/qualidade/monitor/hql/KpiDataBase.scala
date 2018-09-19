package br.com.qualidade.monitor.hql


object KpiDataBase extends Enumeration {

  protected case class Val(hql: String) extends super.Val {}

  val cdSemana = Val(
    s"""
       |select cast(from_unixtime(unix_timestamp('{{dt_foto}}','yyyy-MM-dd'),'u')as int) as cd_smna
    """.stripMargin
  )
}