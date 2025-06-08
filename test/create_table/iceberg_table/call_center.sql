create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists call_center;
create table if not exists call_center(
      cc_call_center_sk int
,     cc_call_center_id char(16)
,     cc_rec_start_date date
,     cc_rec_end_date date
,     cc_closed_date_sk int
,     cc_open_date_sk int
,     cc_name varchar(50)
,     cc_class varchar(50)
,     cc_employees int
,     cc_sq_ft int
,     cc_hours char(20)
,     cc_manager varchar(40)
,     cc_mkt_id int
,     cc_mkt_class char(50)
,     cc_mkt_desc varchar(100)
,     cc_market_manager varchar(40)
,     cc_division int
,     cc_division_name varchar(50)
,     cc_company int
,     cc_company_name char(50)
,     cc_street_number char(10)
,     cc_street_name varchar(60)
,     cc_street_type char(15)
,     cc_suite_number char(10)
,     cc_city varchar(60)
,     cc_county varchar(30)
,     cc_state char(2)
,     cc_zip char(10)
,     cc_country varchar(20)
,     cc_gmt_offset decimal(5,2)
,     cc_tax_percentage decimal(5,2)
)
USING iceberg;
