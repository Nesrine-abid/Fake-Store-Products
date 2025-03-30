-- Identify the quarter date range
WITH last_quarter AS (
  SELECT
    DATE_TRUNC(CURRENT_DATE(), QUARTER) AS current_quarter_start,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY) AS previous_quarter_end,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 QUARTER) AS previous_quarter_start
),

-- Get top 10 products by sales volume in last quarter
top_products AS (
  SELECT
    ft.product_id,
    dp.product_name,
    dp.product_category,
    SUM(ft.quantity) AS total_quantity,
    SUM(ft.product_revenue) AS total_revenue
  FROM
    `notional-data-455119-r6.fake_products.fact_transactions` ft
  JOIN
    `notional-data-455119-r6.fake_products.dim_date` d ON ft.date_id = d.date_id
  JOIN
    `notional-data-455119-r6.fake_products.dim_product` dp ON ft.product_id = dp.product_id
  JOIN
    last_quarter lq ON TRUE
  WHERE
    d.full_date BETWEEN lq.previous_quarter_start AND lq.previous_quarter_end
    AND ft.action_type = '6' -- Completed purchase
  GROUP BY
    ft.product_id, dp.product_name, dp.product_category
  ORDER BY
    total_quantity DESC
  LIMIT 10
),

-- Get monthly sales data for these products over last 6 months
monthly_sales AS (
  SELECT
    ft.product_id,
    d.year,
    d.month,
    FORMAT_DATE('%Y-%m', d.full_date) AS year_month,
    SUM(ft.quantity) AS monthly_quantity,
    SUM(ft.product_revenue) AS monthly_revenue
  FROM
    `notional-data-455119-r6.fake_products.fact_transactions` ft
  JOIN
    `notional-data-455119-r6.fake_products.dim_date` d ON ft.date_id = d.date_id
  WHERE
    ft.product_id IN (SELECT product_id FROM top_products)
    AND d.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    AND ft.action_type = '6' -- Completed purchase
  GROUP BY
    ft.product_id, d.year, d.month, year_month
  ORDER BY
    ft.product_id, year_month
),

-- Calculate month-over-month growth rates
growth_analysis AS (
  SELECT
    ms.*,
    LAG(ms.monthly_quantity) OVER (PARTITION BY ms.product_id ORDER BY ms.year_month) AS prev_month_quantity,
    LAG(ms.monthly_revenue) OVER (PARTITION BY ms.product_id ORDER BY ms.year_month) AS prev_month_revenue,
    CASE
      WHEN LAG(ms.monthly_quantity) OVER (PARTITION BY ms.product_id ORDER BY ms.year_month) > 0 
      THEN (ms.monthly_quantity - LAG(ms.monthly_quantity) OVER (PARTITION BY ms.product_id ORDER BY ms.year_month)) 
           / LAG(ms.monthly_quantity) OVER (PARTITION BY ms.product_id ORDER BY ms.year_month) * 100
      ELSE NULL
    END AS quantity_growth_pct,
    CASE
      WHEN LAG(ms.monthly_revenue) OVER (PARTITION BY ms.product_id ORDER BY ms.year_month) > 0 
      THEN (ms.monthly_revenue - LAG(ms.monthly_revenue) OVER (PARTITION BY ms.product_id ORDER BY ms.year_month)) 
           / LAG(ms.monthly_revenue) OVER (PARTITION BY ms.product_id ORDER BY ms.year_month) * 100
      ELSE NULL
    END AS revenue_growth_pct
  FROM
    monthly_sales ms
)

-- Final result with product details and growth trend classification
SELECT
  ga.year_month,
  tp.product_id,
  tp.product_name,
  tp.product_category,
  ga.monthly_quantity,
  ga.monthly_revenue,
  ROUND(ga.quantity_growth_pct, 2) AS quantity_growth_pct,
  ROUND(ga.revenue_growth_pct, 2) AS revenue_growth_pct,
  -- Classify growth trends
  CASE
    WHEN ga.quantity_growth_pct > 25 THEN 'Strong Growth'
    WHEN ga.quantity_growth_pct BETWEEN 10 AND 25 THEN 'Moderate Growth'
    WHEN ga.quantity_growth_pct BETWEEN -10 AND 10 THEN 'Stable'
    WHEN ga.quantity_growth_pct BETWEEN -25 AND -10 THEN 'Moderate Decline'
    WHEN ga.quantity_growth_pct < -25 THEN 'Significant Decline'
    ELSE 'First Month Data'
  END AS growth_trend,
  -- Identify significant fluctuations
  CASE
    WHEN ABS(ga.quantity_growth_pct) > 25 THEN 'Significant Fluctuation'
    ELSE 'Normal Variation'
  END AS fluctuation_status
FROM
  growth_analysis ga
JOIN
  top_products tp ON ga.product_id = tp.product_id
ORDER BY
  tp.total_quantity DESC, ga.product_id, ga.year_month;