-- Step 1: Identify top 5 customers by total purchase amount in the past year
WITH top_customers AS (
  SELECT
    ft.user_id,
    du.country,
    du.device_category,
    SUM(ft.transaction_revenue) AS total_revenue,
    COUNT(DISTINCT ft.transaction_id) AS transaction_count
  FROM
    `notional-data-455119-r6.fake_products.fact_transactions` ft
  JOIN
    `notional-data-455119-r6.fake_products.dim_date` d ON ft.date_id = d.date_id
  JOIN
    `notional-data-455119-r6.fake_products.dim_user` du ON ft.user_id = du.user_id
  WHERE
    d.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    AND ft.action_type = '6' -- Completed purchase
  GROUP BY
    ft.user_id, du.country, du.device_category
  ORDER BY
    total_revenue DESC
  LIMIT 5
),

-- Step 2: Get monthly purchases for these customers
monthly_purchases AS (
  SELECT
    ft.user_id,
    d.year,
    d.month,
    FORMAT_DATE('%Y-%m', d.full_date) AS year_month,
    SUM(ft.transaction_revenue) AS monthly_revenue,
    COUNT(DISTINCT ft.transaction_id) AS transaction_count
  FROM
    `notional-data-455119-r6.fake_products.fact_transactions` ft
  JOIN
    `notional-data-455119-r6.fake_products.dim_date` d ON ft.date_id = d.date_id
  WHERE
    ft.user_id IN (SELECT user_id FROM top_customers)
    AND d.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    AND ft.action_type = '6' -- Completed purchase
  GROUP BY
    ft.user_id, d.year, d.month, year_month
  ORDER BY
    ft.user_id, year_month
)

-- Step 3: Final result with seasonal patterns identification
SELECT
  tc.user_id,
  tc.country,
  tc.device_category,
  mp.year_month,
  mp.monthly_revenue,
  mp.transaction_count,
  tc.total_revenue AS yearly_revenue,
  mp.monthly_revenue / tc.total_revenue AS pct_of_yearly_spend,
  -- Identify seasonality
  CASE
    WHEN mp.monthly_revenue > (tc.total_revenue / 12) * 1.5 THEN 'High Spending Month'
    WHEN mp.monthly_revenue < (tc.total_revenue / 12) * 0.5 THEN 'Low Spending Month'
    ELSE 'Normal Spending Month'
  END AS spending_pattern,
  -- Calculate month-over-month changes
  (mp.monthly_revenue - LAG(mp.monthly_revenue) OVER (PARTITION BY mp.user_id ORDER BY mp.year_month)) 
    / NULLIF(LAG(mp.monthly_revenue) OVER (PARTITION BY mp.user_id ORDER BY mp.year_month), 0) * 100 AS mom_change_pct
FROM
  monthly_purchases mp
JOIN
  top_customers tc ON mp.user_id = tc.user_id
ORDER BY
  tc.total_revenue DESC, mp.user_id, mp.year_month;