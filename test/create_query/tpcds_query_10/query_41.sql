select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 730 and 730+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'thistle' or i_color = 'lemon') and 
        (i_units = 'N/A' or i_units = 'Each') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'olive' or i_color = 'ghost') and
        (i_units = 'Case' or i_units = 'Pound') and
        (i_size = 'large' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'linen' or i_color = 'powder') and
        (i_units = 'Oz' or i_units = 'Carton') and
        (i_size = 'medium' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'white' or i_color = 'dim') and
        (i_units = 'Cup' or i_units = 'Pallet') and
        (i_size = 'N/A' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'cream' or i_color = 'khaki') and 
        (i_units = 'Tbl' or i_units = 'Dram') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'magenta' or i_color = 'orchid') and
        (i_units = 'Gross' or i_units = 'Lb') and
        (i_size = 'large' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'navy' or i_color = 'floral') and
        (i_units = 'Tsp' or i_units = 'Ounce') and
        (i_size = 'medium' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'pink' or i_color = 'lime') and
        (i_units = 'Dozen' or i_units = 'Gram') and
        (i_size = 'N/A' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100;

