create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists time_dim;
create table if not exists time_dim(
      t_time_sk int
,     t_time_id char(16)
,     t_time int
,     t_hour int
,     t_minute int
,     t_second int
,     t_am_pm char(2)
,     t_shift char(20)
,     t_sub_shift char(20)
,     t_meal_time char(20)
)
USING iceberg;
