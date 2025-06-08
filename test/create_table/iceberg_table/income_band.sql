create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists income_band;
create table if not exists income_band(
      ib_income_band_sk int
,     ib_lower_bound int
,     ib_upper_bound int
)
USING iceberg;
