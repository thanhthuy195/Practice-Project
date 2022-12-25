-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


--Lưu ý chung: với Bigquery thì mình có thể groupby, orderby 1,2,3(1,2,3() ở đây là thứ tự của column mà mình select nhé
--Thụt dòng cho từng đoạn, từng phần để dễ nhìn hơn

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
--a sẽ chỉnh lại bài, thụt dòng theo 1 nguyên tắc, e coi coi dễ nhìn hơn k, mắt mình sẽ thuận khi nhìn dọc hơn là nhìn ngang


SELECT FORMAT_DATE('%Y%m', (parse_date('%Y%m%d',date))) as month
    , count(totals.visits) as visits
    , sum(totals.pageviews) as pageviews
    , sum(totals.transactions) as transactions
    , round(sum(totals.totalTransactionRevenue)/1000000,2) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0101' and '0331'
Group by month  --group by 1
Order by month; --order by 1

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

SELECT trafficSource.source as source
      ,sum(totals.visits) as total_visit
      ,count(totals.bounces) as total_no_of_bounces
      ,sum(totals.bounces)/sum(totals.visits)*100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
Group by 1
Order by 2 desc;

-- Query 3: Revenue by traffic source by week, by month in June 2017

With week_table as 
    (SELECT FORMAT_DATE('%Y%U', (parse_date('%Y%m%d',date))) as time
          ,case when FORMAT_DATE('%Y%U', (parse_date('%Y%m%d',date))) like '2017%' then 'week' --'week' as time_type
            Else null end as time_type
          ,trafficSource.source as source
          ,sum(totals.totalTransactionRevenue) as revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    Where _table_suffix between '0601' and '0630'
    Group by date, source
    Order by time)
,
month_table as 
    (SELECT FORMAT_DATE('%Y%m', (parse_date('%Y%m%d',date))) as time
          ,case when FORMAT_DATE('%Y%m', (parse_date('%Y%m%d',date))) like '201706' then 'month' --'month' as time_type
          Else null end as time_type
          ,trafficSource.source as source
          ,sum(totals.totalTransactionRevenue) as revenue
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    Where _table_suffix between '0601' and '0630'
    Group by date, source
    Order by time)

Select * from week_table
union all
select * from month_table;
--a chỉnh lại khúc ghi time_type nha
with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

SELECT FORMAT_DATE('%Y%m', (parse_date('%Y%m%d',date))) as month, 
      sum(totals.pageviews)/count(case when totals.transactions >= 1 then 1 end) as avg_pageviews_purchase,
      sum(totals.pageviews)/count(case when totals.transactions is Null then 1 end) as avg_pageviews_non_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0601' and '0731'
Group by month
Order by month;

--cách khác dùng CTE
with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

SELECT FORMAT_DATE('%Y%m', (parse_date('%Y%m%d',date))) as month,
      sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0701' and '0731'
and totals.transactions >=1
Group by month;


-- Query 06: Average amount of money spent per session
#standardSQL

SELECT FORMAT_DATE('%Y%m', (parse_date('%Y%m%d',date))) as month,
      sum(totals.totalTransactionRevenue)/count(totals.transactions) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0701' and '0731'
and totals.transactions is not NULL
Group by month;



-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

--k nên đặt tên CTE là cte hoặc ABC,nên đặt tên viết tắt, mà nhìn vào mình có thể hiểu đc CTE đó đang lấy data gì

with table1 as 
    (select distinct fullvisitorId
          ,v2ProductName
          ,sum(productQuantity) as quantity
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits) AS hits,
      UNNEST(product) as product
    where _table_suffix between '0701' and '0731'
    and productRevenue is not null
    and v2ProductName = "YouTube Men's Vintage Henley"
    Group by fullvisitorId, v2ProductName
    order by quantity)
,
table2 as 
    (select distinct fullvisitorId
          ,v2ProductName
          ,sum(productQuantity) as quantity
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits) AS hits,
      UNNEST(product) as product
    where _table_suffix between '0701' and '0731'
    and productRevenue is not null
    Group by fullvisitorId, v2ProductName
    order by quantity)

Select distinct v2ProductName as other_purchased_products
      ,quantity
from table2
where fullvisitorId IN (select fullvisitorId from table1)   --khúc này e dùng IN, nghĩa là dùng subquery rồi, thì e có thể bỏ nguyên phần cte table1 vào phần in này luôn
group by other_purchased_products, quantity
Order by quantity desc;

--cách subquery:
select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and hits.eCommerceAction.action_type = '6')
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc

--cách CTE
with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL


SELECT FORMAT_DATE('%Y%m', (parse_date('%Y%m%d',date))) as month,
      Count(case when eCommerceAction.action_type = '2' then 1 end) as num_product_view,
      Count(case when eCommerceAction.action_type = '3' then 1 end) as num_addtocart,
      Count(case when eCommerceAction.action_type = '6' then 1 end) as num_purchase,
      (Count(case when eCommerceAction.action_type = '3' then 1 end)/Count(case when eCommerceAction.action_type = '2' then 1 end)*100) as add_to_cart_rate,
      (Count(case when eCommerceAction.action_type = '6' then 1 end)/Count(case when eCommerceAction.action_type = '2' then 1 end)*100) as purchase_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) AS hits
where _table_suffix between '0101' and '0331'
group by month
Order by month;


ghi như trên nhìn sẽ hơi khó nha,a tách ra 2 step

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data


                                    ---VERY GOOD---
