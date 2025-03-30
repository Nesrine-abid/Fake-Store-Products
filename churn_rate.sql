-- Step 1: Identify active users by month
WITH monthly_active_users AS (
  SELECT
    FORMAT_DATE('%Y-%m', d.full_date) AS year_month,
    d.year,
    d.month,
    fs.user_id
  FROM
    `your_project.your_dataset.fact_sessions` fs
  JOIN
    `your_project.your_dataset.dim_date` d ON fs.date_id = d.date_id
  WHERE
    d.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)
  GROUP BY
    year_month, d.year, d.month, fs.user_id
),

-- Step 2: Count active users by month
active_user_counts AS (
  SELECT
    year_month,
    year,
    month,
    COUNT(DISTINCT user_id) AS active_users
  FROM
    monthly_active_users
  GROUP BY
    year_month, year, month
  ORDER BY
    year, month
),

-- Step 3: Calculate retained and churned users
user_retention AS (
  SELECT
    current_month.year_month AS current_month,
    previous_month.year_month AS previous_month,
    current_month.active_users AS current_active_users,
    previous_month.active_users AS previous_active_users,
    (SELECT COUNT(DISTINCT current_users.user_id)
     FROM monthly_active_users current_users
     WHERE current_users.year_month = current_month.year_month
     AND current_users.user_id IN (
       SELECT previous_users.user_id
       FROM monthly_active_users previous_users
       WHERE previous_users.year_month = previous_month.year_month
     )) AS retained_users
  FROM
    active_user_counts current_month
  JOIN
    active_user_counts previous_month
  ON
    (current_month.year * 12 + current_month.month) = 
    (previous_month.year * 12 + previous_month.month + 1)
),

-- Step 4: Add context from traffic sources and device data
contextual_data AS (
  SELECT
    FORMAT_DATE('%Y-%m', d.full_date) AS year_month,
    dts.source,
    dts.medium,
    du.device_category,
    du.country,
    COUNT(DISTINCT fs.session_id) AS sessions
  FROM
    `your_project.your_dataset.fact_sessions` fs
  JOIN
    `your_project.your_dataset.dim_date` d ON fs.date_id = d.date_id
  JOIN
    `your_project.your_dataset.dim_traffic_source` dts ON fs.traffic_source_id = dts.traffic_source_id
  JOIN
    `your_project.your_dataset.dim_user` du ON fs.user_id = du.user_id
  WHERE
    d.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)
  GROUP BY
    year_month, dts.source, dts.medium, du.device_category, du.country
)

-- Step 5: Final result with churn rate calculations and trend analysis
SELECT
  ur.current_month,
  ur.previous_month,
  ur.previous_active_users,
  ur.current_active_users,
  ur.retained_users,
  ur.previous_active_users - ur.retained_users AS churned_users,
  (ur.previous_active_users - ur.retained_users) / ur.previous_active_users AS churn_rate,
  ur.retained_users / ur.previous_active_users AS retention_rate,
  (ur.current_active_users - ur.retained_users) AS new_users,
  (ur.current_active_users - ur.previous_active_users) / ur.previous_active_users AS growth_rate,
  
  -- Identify churn trend compared to previous month
  CASE
    WHEN LAG((ur.previous_active_users - ur.retained_users) / ur.previous_active_users) 
         OVER (ORDER BY ur.current_month) < 
         (ur.previous_active_users - ur.retained_users) / ur.previous_active_users
    THEN 'Increasing Churn'
    WHEN LAG((ur.previous_active_users - ur.retained_users) / ur.previous_active_users) 
         OVER (ORDER BY ur.current_month) > 
         (ur.previous_active_users - ur.retained_users) / ur.previous_active_users
    THEN 'Decreasing Churn'
    ELSE 'Stable Churn'
  END AS churn_trend,
  
  -- Calculate 3-month rolling average churn rate for trend smoothing
  AVG((ur.previous_active_users - ur.retained_users) / ur.previous_active_users) 
    OVER (ORDER BY ur.current_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg_churn_rate,
  
  -- Identify peak traffic channels for the month
  (SELECT source FROM contextual_data 
   WHERE year_month = ur.current_month 
   GROUP BY source ORDER BY SUM(sessions) DESC LIMIT 1) AS top_traffic_source,
   
  -- Identify device trend for the month
  (SELECT device_category FROM contextual_data 
   WHERE year_month = ur.current_month 
   GROUP BY device_category ORDER BY SUM(sessions) DESC LIMIT 1) AS top_device_category
FROM
  user_retention ur
ORDER BY
  ur.current_month;