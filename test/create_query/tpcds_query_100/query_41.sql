select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 998 and 998+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'ivory' or i_color = 'salmon') and 
        (i_units = 'Ounce' or i_units = 'Lb') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'puff' or i_color = 'green') and
        (i_units = 'Tbl' or i_units = 'Dozen') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'cornflower' or i_color = 'lawn') and
        (i_units = 'Case' or i_units = 'Unknown') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'turquoise' or i_color = 'bisque') and
        (i_units = 'Cup' or i_units = 'Ton') and
        (i_size = 'medium' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'misty' or i_color = 'forest') and 
        (i_units = 'Box' or i_units = 'Pallet') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'lavender' or i_color = 'dim') and
        (i_units = 'Tsp' or i_units = 'Gram') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'antique' or i_color = 'firebrick') and
        (i_units = 'Each' or i_units = 'N/A') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'blush' or i_color = 'indian') and
        (i_units = 'Dram' or i_units = 'Carton') and
        (i_size = 'medium' or i_size = 'economy')
        )))) > 0
 order by i_product_name
 limit 100;

