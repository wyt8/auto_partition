create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists date_dim;
create table if not exists date_dim(
      d_date_sk int
,     d_date_id char(16)
,     d_date date
,     d_month_seq int
,     d_week_seq int
,     d_quarter_seq int
,     d_year int
,     d_dow int
,     d_moy int
,     d_dom int
,     d_qoy int
,     d_fy_year int
,     d_fy_quarter_seq int
,     d_fy_week_seq int
,     d_day_name char(9)
,     d_quarter_name char(6)
,     d_holiday char(1)
,     d_weekend char(1)
,     d_following_holiday char(1)
,     d_first_dom int
,     d_last_dom int
,     d_same_day_ly int
,     d_same_day_lq int
,     d_current_day char(1)
,     d_current_week char(1)
,     d_current_month char(1)
,     d_current_quarter char(1)
,     d_current_year char(1)
)
USING iceberg;
