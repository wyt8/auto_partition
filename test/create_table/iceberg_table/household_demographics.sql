create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists household_demographics;
create table if not exists household_demographics(
      hd_demo_sk int
,     hd_income_band_sk int
,     hd_buy_potential char(15)
,     hd_dep_count int
,     hd_vehicle_count int
)
USING iceberg;
