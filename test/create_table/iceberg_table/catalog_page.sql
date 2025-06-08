create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists catalog_page;
create table if not exists catalog_page(
      cp_catalog_page_sk int
,     cp_catalog_page_id char(16)
,     cp_start_date_sk int
,     cp_end_date_sk int
,     cp_department varchar(50)
,     cp_catalog_number int
,     cp_catalog_page_number int
,     cp_description varchar(100)
,     cp_type varchar(100)
)
USING iceberg;
