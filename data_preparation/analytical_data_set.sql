-- Databricks notebook source
create or replace table circlek_db.suraj_demand_forecasting_demo as 
select
  t.MarketBasket_Header_Key,
  t.sys_environment_name,
  t.Product_Key,
  t.transaction_date,
  p.product_key as product_key_lp,
  s.site_number,
  t.unit_price,
  t.Total_Amount,
  t.total_quantity,
  t.Department_desc as category_desc,
  c.cluster,
  i.scope,
  i.price_family,
  e.week_end
from
  dl_src_ck_datascience.transaction_item_details t
  join dl_localized_pricing_all_bu.site s on t.Site_Id = s.site_number
  and t.sys_environment_name = s.sys_environment_name
  join dl_localized_pricing_all_bu.product p on t.Product_Key = p.mb_product_key
  and t.sys_environment_name = p.environment_name
  join circlek_db.grouped_store_list c on s.site_number = c.site_id
  join circlek_db.lp_dashboard_items_scope i on p.product_key = i.product_key
  join circlek_db.fiscal_calendar_updated e on t.transaction_date = e.date
where
  t.Transaction_Type_Key in (2, 3)
  and t.6sig_outlier_score != 1
  and t.Promotion_Id = 0
  and t.transaction_date between '2017-07-01' and '2022-06-30'
  and s.division_desc = 'Central Division'
  and i.category_name = "Other Bevrgs (45)"
  and i.price_family = '710ML - SPORTS DRINKS - PEPSI - GATORADE'
  and c.cluster = 1
--   and t.unit_price = 3.29

-- COMMAND ----------

select price_family, Product_Key, unit_price, week_end, count(unit_price) from circlek_db.suraj_demand_forecasting_demo
where week_end = "2022-07-17"
group by price_family, Product_Key, week_end, unit_price
order by 1, 2, 3, 5 desc

-- COMMAND ----------

with quantity_amount_summary as (
select
  price_family,
  week_end,
  sum(total_quantity) total_quantity,
  sum(total_amount) total_amount
from
  circlek_db.suraj_demand_forecasting_demo
group by
  1, 2
order by 1 desc, 2 asc),

weekly_price as (
select 
  price_family,
  week_end,
  unit_price,
  count(unit_price) as price_count
from
  circlek_db.suraj_demand_forecasting_demo 
group by 1, 2, 3
),

weekly_price_summ as(
select 
  price_family,
  week_end,
  unit_price,
  price_count,
  row_number() over(partition by price_family, week_end order by price_count desc) as price_rank
from weekly_price
)

select 
  qa.price_family,
  qa.week_end,
  qa.total_quantity,
  qa.total_amount,
  p.unit_price,
  p.price_rank
from quantity_amount_summary qa
inner join weekly_price_summ p on qa.price_family = p.price_family 
  and qa.week_end = p.week_end
where p.price_rank = 1

-- COMMAND ----------

select * from circlek_db.suraj_demand_forecasting_demo

-- COMMAND ----------


