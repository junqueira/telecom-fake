drop table if exists p_desenvolvimento_db.tbgd_tdq_work_qlto_dia_cgi_kpi;
-- estacao radio base
CREATE TABLE p_desenvolvimento_db.tbgd_tdq_work_qlto_dia_cgi_kpi(
  `cd_sem` int,
  `qt_total_erb` bigint,
  `qt_total_cgi` bigint,
  `qt_total_site` bigint,
  `qt_duplicidades_erb` bigint,
  `qt_duplicidades_site` bigint,
  `qt_muda_posicao` bigint,
  `qt_ibge_null` bigint,
  `qt_lati_long_null` bigint,
  `qt_erb_fora_brasil` bigint
)
PARTITIONED BY (
  `dt_foto` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS ORC;