drop table if exists desenv.qlto_dia_kpi;
CREATE TABLE desenv.qlto_dia_kpi(
  `cd_week` int,
  `qt_total` bigint,
  `qt_duplicity` bigint,
  `qt_change_position` bigint,
  `qt_null` bigint,
  `qt_lati_long_null` bigint,
  `qt_outside_region` bigint
)
PARTITIONED BY (
  `dt_foto` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS ORC;