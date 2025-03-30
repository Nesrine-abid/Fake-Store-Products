-- Calculate monthly sessions and conversions by traffic source
WITH monthly_traffic_data AS (
  SELECT
    d.year,
    d.month,
    FORMAT_DATE('%Y-%m', d.full_date) AS year_month,
    fs.traffic_source_id,
    COUNT(DISTINCT fs.session_id) AS sessions,
    COUNT(DISTINCT CASE WHEN fs.transactions > 0 THEN fs.session_id END) AS converting_sessions,
    SUM(fs.transaction_revenue) AS revenue
  FROM
    `notional-data-455119-r6.fake_products.fact_sessions` fs
  JOIN
    `notional-data-455119-r6.fake_products.dim_date` d ON fs.date_id = d.date_id
  WHERE
    d.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
  GROUP BY
    d.year, d.month, year_month, fs.traffic_source_id
),

-- Calculate conversion rates and month-over-month changes
conversion_trends AS (
  SELECT
    mtd.*,
    SAFE_DIVIDE(mtd.converting_sessions, mtd.sessions) AS conversion_rate,
    SAFE_DIVIDE(mtd.revenue, mtd.sessions) AS revenue_per_session,
    LAG(SAFE_DIVIDE(mtd.converting_sessions, mtd.sessions)) OVER (
      PARTITION BY mtd.traffic_source_id 
      ORDER BY mtd.year_month
    ) AS prev_month_conversion_rate,
    -- Calculate absolute change in conversion rate (percentage points)
    CASE 
      WHEN LAG(SAFE_DIVIDE(mtd.converting_sessions, mtd.sessions)) OVER (
        PARTITION BY mtd.traffic_source_id 
        ORDER BY mtd.year_month
      ) IS NOT NULL
      THEN SAFE_DIVIDE(mtd.converting_sessions, mtd.sessions) - 
           LAG(SAFE_DIVIDE(mtd.converting_sessions, mtd.sessions)) OVER (
             PARTITION BY mtd.traffic_source_id 
             ORDER BY mtd.year_month
           )
      ELSE NULL
    END AS conversion_rate_change,
    -- Calculate percentage change in conversion rate
    CASE
      WHEN LAG(SAFE_DIVIDE(mtd.converting_sessions, mtd.sessions)) OVER (
        PARTITION BY mtd.traffic_source_id 
        ORDER BY mtd.year_month
      ) > 0
      THEN (SAFE_DIVIDE(mtd.converting_sessions, mtd.sessions) - 
            LAG(SAFE_DIVIDE(mtd.converting_sessions, mtd.sessions)) OVER (
              PARTITION BY mtd.traffic_source_id 
              ORDER BY mtd.year_month
            )) / 
            LAG(SAFE_DIVIDE(mtd.converting_sessions, mtd.sessions)) OVER (
              PARTITION BY mtd.traffic_source_id 
              ORDER BY mtd.year_month
            ) * 100
      ELSE NULL
    END AS conversion_rate_pct_change
  FROM
    monthly_traffic_data mtd
)

-- Final result with traffic source details
SELECT
  ct.year_month,
  dts.source,
  dts.medium,
  dts.campaign,
  ct.sessions,
  ct.converting_sessions,
  ROUND(ct.conversion_rate * 100, 2) AS conversion_rate_pct,
  ROUND(ct.revenue_per_session, 2) AS revenue_per_session,
  ROUND(ct.conversion_rate_change * 100, 2) AS conversion_rate_pct_point_change,
  ROUND(ct.conversion_rate_pct_change, 2) AS conversion_rate_pct_change,
  -- Classify conversion rate trends
  CASE
    WHEN ct.conversion_rate_pct_change > 15 THEN 'Strong Improvement'
    WHEN ct.conversion_rate_pct_change BETWEEN 5 AND 15 THEN 'Moderate Improvement'
    WHEN ct.conversion_rate_pct_change BETWEEN -5 AND 5 THEN 'Stable'
    WHEN ct.conversion_rate_pct_change BETWEEN -15 AND -5 THEN 'Moderate Decline'
    WHEN ct.conversion_rate_pct_change < -15 THEN 'Significant Decline'
    ELSE 'First Month Data'
  END AS conversion_trend
FROM
  conversion_trends ct
JOIN
  `notional-data-455119-r6.fake_products.dim_traffic_source` dts ON ct.traffic_source_id = dts.traffic_source_id
WHERE
  ct.sessions >= 100 
ORDER BY
  ct.conversion_rate DESC, dts.source, dts.medium, ct.year_month;