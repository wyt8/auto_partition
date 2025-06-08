select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 86 and 86+10 
             or ss_coupon_amt between 28 and 28+1000
             or ss_wholesale_cost between 37 and 37+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 51 and 51+10
          or ss_coupon_amt between 11511 and 11511+1000
          or ss_wholesale_cost between 41 and 41+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 116 and 116+10
          or ss_coupon_amt between 560 and 560+1000
          or ss_wholesale_cost between 42 and 42+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 114 and 114+10
          or ss_coupon_amt between 5914 and 5914+1000
          or ss_wholesale_cost between 39 and 39+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 54 and 54+10
          or ss_coupon_amt between 8198 and 8198+1000
          or ss_wholesale_cost between 68 and 68+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 135 and 135+10
          or ss_coupon_amt between 10184 and 10184+1000
          or ss_wholesale_cost between 28 and 28+20)) B6
limit 100;

