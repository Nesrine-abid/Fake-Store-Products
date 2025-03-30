-- Step 1: Calculate engagement metrics by user
WITH user_engagement AS (
  SELECT
    fs.user_id,
    COUNT(DISTINCT fs.session_id) AS total_sessions,
    SUM(fs.pageviews) AS total_pageviews,
    AVG(fs.time_on_site) AS avg_session_duration,
    AVG(fs.pageviews) AS avg_pageviews_per_session,
    COUNT(DISTINCT fs.date_id) AS days_active,
    MAX(d.full_date) AS last_active_date
  FROM
    `notional-data-455119-r6.fake_products.fact_sessions` fs
  JOIN
    `notional-data-455119-r6.fake_products.dim_date` d ON fs.date_id = d.date_id
  WHERE
    d.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY
    fs.user_id
),

-- Step 2: Calculate revenue metrics by user
user_revenue AS (
  SELECT
    ft.user_id,
    COUNT(DISTINCT ft.transaction_id) AS total_transactions,
    SUM(ft.transaction_revenue) AS total_revenue,
    SUM(ft.transaction_revenue) / COUNT(DISTINCT ft.transaction_id) AS avg_transaction_value,
    COUNT(DISTINCT ft.date_id) AS purchase_days,
    MAX(d.full_date) AS last_purchase_date
  FROM
    `notional-data-455119-r6.fake_products.fact_transactions` ft
  JOIN
    `notional-data-455119-r6.fake_products.dim_date` d ON ft.date_id = d.date_id
  WHERE
    ft.date_id >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND ft.action_type = '6' -- Completed purchase
  GROUP BY
    ft.user_id
),

-- Step 3: Create engagement segments using percentiles for better distribution
engagement_segments AS (
  SELECT
    ue.user_id,
    ue.total_sessions,
    ue.total_pageviews,
    ue.avg_session_duration,
    ue.avg_pageviews_per_session,
    ue.days_active,
    ue.last_active_date,
    -- Create percentile segments for each metric
    NTILE(3) OVER (ORDER BY ue.total_pageviews) AS pageview_segment,
    NTILE(3) OVER (ORDER BY ue.avg_session_duration) AS duration_segment,
    NTILE(3) OVER (ORDER BY ue.days_active) AS frequency_segment
  FROM
    user_engagement ue
),

-- Step 4: Determine overall engagement segment
user_segments AS (
  SELECT
    es.user_id,
    es.total_sessions,
    es.total_pageviews,
    es.avg_session_duration,
    es.days_active,
    es.last_active_date,
    -- Calculate overall engagement score and segment
    (es.pageview_segment + es.duration_segment + es.frequency_segment) / 3.0 AS engagement_score,
    CASE
      WHEN (es.pageview_segment + es.duration_segment + es.frequency_segment) / 3.0 <= 1.2 THEN 'Low'
      WHEN (es.pageview_segment + es.duration_segment + es.frequency_segment) / 3.0 <= 2.2 THEN 'Medium'
      ELSE 'High'
    END AS engagement_segment
  FROM
    engagement_segments es
),

-- Step 5: Combine engagement and revenue data at user level
user_combined AS (
  SELECT
    us.user_id,
    us.engagement_segment,
    us.engagement_score,
    us.total_sessions,
    us.total_pageviews,
    us.avg_session_duration,
    us.days_active,
    DATE_DIFF(CURRENT_DATE(), us.last_active_date, DAY) AS days_since_last_active,
    ur.total_transactions,
    ur.total_revenue,
    ur.avg_transaction_value,
    ur.purchase_days,
    IFNULL(ur.last_purchase_date, DATE('1900-01-01')) AS last_purchase_date,
    CASE 
      WHEN ur.last_purchase_date IS NOT NULL 
      THEN DATE_DIFF(CURRENT_DATE(), ur.last_purchase_date, DAY)
      ELSE NULL
    END AS days_since_last_purchase
  FROM
    user_segments us
  LEFT JOIN
    user_revenue ur ON us.user_id = ur.user_id
)

-- Step 6: Calculate segment aggregates and correlations
SELECT
  uc.engagement_segment,
  COUNT(DISTINCT uc.user_id) AS user_count,
  COUNTIF(uc.total_transactions > 0) AS purchasing_user_count,
  COUNTIF(uc.total_transactions > 0) / COUNT(DISTINCT uc.user_id) AS conversion_rate,
  
  -- Engagement metrics by segment
  AVG(uc.total_sessions) AS avg_sessions,
  AVG(uc.total_pageviews) AS avg_pageviews,
  AVG(uc.avg_session_duration) AS avg_time_on_site,
  AVG(uc.days_active) AS avg_days_active,
  AVG(uc.days_since_last_active) AS avg_days_since_last_active,
  
  -- Revenue metrics by segment
  AVG(IFNULL(uc.total_transactions, 0)) AS avg_transactions,
  AVG(IFNULL(uc.total_revenue, 0)) AS avg_revenue,
  AVG(IFNULL(uc.avg_transaction_value, 0)) AS avg_transaction_value,
  SUM(IFNULL(uc.total_revenue, 0)) / COUNT(DISTINCT uc.user_id) AS revenue_per_user,
  SUM(IFNULL(uc.total_revenue, 0)) AS total_segment_revenue,
  SUM(IFNULL(uc.total_revenue, 0)) / (SELECT SUM(IFNULL(total_revenue, 0)) FROM user_combined) AS pct_of_total_revenue,
  
  -- Purchase frequency metrics
  AVG(IFNULL(uc.purchase_days, 0)) AS avg_purchase_days,
  AVG(IFNULL(uc.days_since_last_purchase, NULL)) AS avg_days_since_purchase,
  
  -- Calculate revenue efficiency (revenue per pageview)
  SUM(IFNULL(uc.total_revenue, 0)) / NULLIF(SUM(uc.total_pageviews), 0) AS revenue_per_pageview,
  
  -- Calculate engagement to revenue ratio
  AVG(IFNULL(uc.total_revenue, 0)) / NULLIF(AVG(uc.engagement_score), 0) AS revenue_to_engagement_ratio
FROM
  user_combined uc
GROUP BY
  uc.engagement_segment
ORDER BY
  CASE uc.engagement_segment 
    WHEN 'Low' THEN 1
    WHEN 'Medium' THEN 2
    WHEN 'High' THEN 3
  END;