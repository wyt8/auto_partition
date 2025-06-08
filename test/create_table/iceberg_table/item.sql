create database if not exists ${SPARK_CATALOG}.${DB};
USE ${SPARK_CATALOG}.${DB};

drop table if exists item;
create table if not exists item(
      i_item_sk int
,     i_item_id char(16)
,     i_rec_start_date date
,     i_rec_end_date date
,     i_item_desc varchar(200)
,     i_current_price decimal(7,2)
,     i_wholesale_cost decimal(7,2)
,     i_brand_id int
,     i_brand char(50)
,     i_class_id int
,     i_class char(50)
,     i_category_id int
,     i_category char(50)
,     i_manufact_id int
,     i_manufact char(50)
,     i_size char(20)
,     i_formulation char(20)
,     i_color char(20)
,     i_units char(10)
,     i_container char(10)
,     i_manager_id int
,     i_product_name char(50)
)
USING iceberg;
