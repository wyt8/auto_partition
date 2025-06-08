create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists ship_mode;
create table if not exists ship_mode(
      sm_ship_mode_sk int
,     sm_ship_mode_id char(16)
,     sm_type char(30)
,     sm_code char(10)
,     sm_carrier char(20)
,     sm_contract char(20)
)
USING iceberg;
