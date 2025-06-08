create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists reason;
create table if not exists reason(
      r_reason_sk int
,     r_reason_id char(16)
,     r_reason_desc char(100)
)
USING iceberg;
