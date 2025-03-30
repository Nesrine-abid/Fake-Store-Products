-- 1. Date Dimension
CREATE OR REPLACE TABLE `notional-data-455119-r6.fake_products.dim_date` AS
SELECT DISTINCT
  PARSE_DATE('%Y%m%d', date) AS date_id,
  PARSE_DATE('%Y%m%d', date) AS full_date,
  EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS year,
  EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', date)) AS month,
  EXTRACT(DAY FROM PARSE_DATE('%Y%m%d', date)) AS day,
  FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', date)) AS day_of_week,
  EXTRACT(QUARTER FROM PARSE_DATE('%Y%m%d', date)) AS quarter
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`;

-- 2. User Dimension
CREATE OR REPLACE TABLE `notional-data-455119-r6.fake_products.dim_user` AS
SELECT DISTINCT
  fullVisitorId AS user_id,
  device.browser AS browser,
  device.operatingSystem AS operating_system,
  device.isMobile AS is_mobile,
  device.deviceCategory AS device_category,
  geoNetwork.country AS country,
  geoNetwork.city AS city
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`;

-- 3. Traffic Source Dimension
CREATE OR REPLACE TABLE `notional-data-455119-r6.fake_products.dim_traffic_source` AS
SELECT DISTINCT
  CONCAT(trafficSource.source, '-', trafficSource.medium, '-', 
         IFNULL(trafficSource.campaign, 'not set'), '-', 
         IFNULL(trafficSource.adContent, 'not set')) AS traffic_source_id,
  trafficSource.source AS source,
  trafficSource.medium AS medium,
  trafficSource.campaign AS campaign,
  trafficSource.adContent AS ad_content,
  trafficSource.keyword AS keyword,
  trafficSource.adwordsClickInfo.adNetworkType AS ad_network_type,
  trafficSource.isTrueDirect AS is_direct
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`;

-- 4. Page Dimension
CREATE OR REPLACE TABLE `notional-data-455119-r6.fake_products.dim_page` AS
WITH page_hits AS (
  SELECT
    hits.page.pagePath AS page_path,
    hits.page.pageTitle AS page_title,
    hits.page.hostname AS hostname
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
  WHERE
    hits.type = 'PAGE'
)
SELECT DISTINCT
  page_path AS page_id,
  page_path,
  page_title,
  hostname,
  REGEXP_EXTRACT(page_path, r'^\/([^\/\?]*)') AS page_section
FROM
  page_hits;

-- 5. Product Dimension (for e-commerce)
CREATE OR REPLACE TABLE `notional-data-455119-r6.fake_products.dim_product` AS
WITH product_hits AS (
  SELECT
    product.v2ProductName AS product_name,
    product.v2ProductCategory AS product_category,
    product.productSKU AS product_sku,
    product.productVariant AS product_variant,
    product.productBrand AS product_brand
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
  CROSS JOIN UNNEST(hits.product) AS product
  WHERE
    hits.eCommerceAction.action_type IN ('2', '3', '4', '5', '6')
)
SELECT DISTINCT
  product_sku AS product_id,
  product_name,
  product_category,
  product_variant,
  product_brand
FROM
  product_hits
WHERE
  product_sku IS NOT NULL;
