CREATE OR REPLACE TABLE `notional-data-455119-r6.fake_products.fact_sessions` AS
SELECT
  CONCAT(fullVisitorId, '-', CAST(visitId AS STRING)) AS session_id,
  fullVisitorId AS user_id,
  PARSE_DATE('%Y%m%d', date) AS date_id,
  CONCAT(trafficSource.source, '-', trafficSource.medium, '-', 
         IFNULL(trafficSource.campaign, 'not set'), '-', 
         IFNULL(trafficSource.adContent, 'not set')) AS traffic_source_id,
  visitNumber AS visit_number,
  totals.visits AS visits,
  totals.hits AS total_hits,
  totals.pageviews AS pageviews,
  totals.bounces AS bounces,
  totals.timeOnSite AS time_on_site,
  totals.transactions AS transactions,
  totals.transactionRevenue/1000000 AS transaction_revenue,
  totals.newVisits AS new_visits
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`;


CREATE OR REPLACE TABLE `notional-data-455119-r6.fake_products.fact_pageviews` AS
SELECT
  TO_HEX(MD5(CONCAT(fullVisitorId, '-', CAST(visitId AS STRING), '-', 
             CAST(hits.hitNumber AS STRING)))) AS pageview_id,
  CONCAT(fullVisitorId, '-', CAST(visitId AS STRING)) AS session_id,
  fullVisitorId AS user_id,
  PARSE_DATE('%Y%m%d', date) AS date_id,
  hits.page.pagePath AS page_id,
  hits.type AS hit_type,
  hits.hitNumber AS hit_number,
  hits.time AS hit_time,
  hits.page.pageTitle AS page_title,
  hits.isEntrance AS is_entrance,
  hits.isExit AS is_exit,
  hits.page.searchKeyword AS search_keyword,
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits
WHERE
  hits.type = 'PAGE';



CREATE OR REPLACE TABLE `notional-data-455119-r6.fake_products.fact_transactions` AS
SELECT
  TO_HEX(MD5(CONCAT(fullVisitorId, '-', CAST(visitId AS STRING), '-', 
             CAST(hits.hitNumber AS STRING), '-', product.productSKU))) AS transaction_item_id,
  CONCAT(fullVisitorId, '-', CAST(visitId AS STRING)) AS session_id,
  fullVisitorId AS user_id,
  PARSE_DATE('%Y%m%d', date) AS date_id,
  product.productSKU AS product_id,
  hits.transaction.transactionId AS transaction_id,
  hits.hitNumber AS hit_number,
  hits.time AS hit_time,
  hits.eCommerceAction.action_type AS action_type,
  product.productQuantity AS quantity,
  product.productPrice/1000000 AS product_price,
  product.localProductRevenue/1000000 AS product_revenue,
  hits.transaction.transactionRevenue/1000000 AS transaction_revenue,
  hits.transaction.transactionTax/1000000 AS transaction_tax,
  hits.transaction.transactionShipping/1000000 AS transaction_shipping
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product
WHERE
  hits.eCommerceAction.action_type IN ('2', '3', '4', '5', '6')
  AND product.productSKU IS NOT NULL;

