sql_demographics_table = """
SELECT 
  id AS ad_id, 
  demos.*
FROM `{staging_table_path}`
CROSS JOIN UNNEST(demographic_distribution) AS demos
"""

sql_spend_table = """
SELECT 
  id AS ad_id,
  currency,
  'upper' AS bound,
  spend.upper_bound AS amount
FROM `{staging_table_path}`
UNION ALL
SELECT 
  id AS ad_id, 
  currency,
  'lower' AS bound,
  spend.lower_bound AS amount
FROM `{staging_table_path}`
UNION ALL
SELECT 
  id AS ad_id, 
  currency,
  'midpoint' AS bound,
  ROUND((spend.lower_bound + spend.upper_bound) / 2, 2) AS amount
FROM `{staging_table_path}`
"""

sql_impressions_table = """
SELECT 
  id AS ad_id, 
  'upper' AS bound,
  impressions.upper_bound AS amount
FROM `{staging_table_path}`
UNION ALL
SELECT 
  id AS ad_id, 
  'lower' AS bound, 
  impressions.lower_bound AS amount
FROM `{staging_table_path}`
UNION ALL
SELECT 
  id AS ad_id, 
  'midpoint' AS bound,
  ROUND((impressions.lower_bound + impressions.upper_bound) / 2) AS amount
FROM `{staging_table_path}`
"""

sql_regions_table = """
SELECT 
  id AS ad_id, 
  regions.*
FROM `{staging_table_path}`
CROSS JOIN UNNEST(region_distribution) AS regions
"""

sql_ads_table = """
SELECT 
  *
  EXCEPT(
    spend,
    currency,
    impressions, 
    region_distribution, 
    demographic_distribution
  )
FROM `{staging_table_path}`
"""

sql_count_table_rows = """
SELECT COUNT(*)
FROM `{project}.{dataset}.{table}`
"""

sql_transform_guardian_table = """
SELECT 
  IF(age_group != '12-17', age_group, '13-17') AS age_group,
  IF(age_group != '12-17', population, 
     SAFE_CAST(ROUND((population / 6) * 5) AS INT64)) AS population
FROM `{project}.{dataset}.{table}`
"""

sql_transform_ons_table = """
WITH
get_min_max_age_and_pop_by_year AS (
SELECT 
  *, 
  SAFE_CAST(SPLIT(TRIM(age_group), '-')[OFFSET(0)] AS INT64) AS min_age, 
  SAFE_CAST(SPLIT(TRIM(age_group), '-')[OFFSET(1)] AS INT64) AS max_age,
  population / 5 AS population_per_year
FROM `{project}.{dataset}.{table}`
WHERE age_group != ' 100 & over' -- ignore special case
),

one_row_per_year AS (
SELECT
  *,
  GENERATE_ARRAY(min_age, max_age) AS years
FROM get_min_max_age_and_pop_by_year 
),

population_by_year AS (
SELECT
  main.gender,
  population_per_year,
  year
FROM one_row_per_year AS main
CROSS JOIN UNNEST(years) AS year
),

include_100plus AS (
SELECT
  *
FROM population_by_year
UNION ALL
SELECT
  gender, 
  population,
  100 AS year
FROM `{project}.{dataset}.{table}`
WHERE age_group = ' 100 & over'
),

add_facebook_age_groups AS (
SELECT
  *,
  CASE
    WHEN year BETWEEN 12 AND 17 THEN '13-17'
    WHEN year BETWEEN 18 AND 24 THEN '18-24'
    WHEN year BETWEEN 25 AND 34 THEN '25-34'
    WHEN year BETWEEN 35 AND 44 THEN '35-44'
    WHEN year BETWEEN 45 AND 54 THEN '45-54'
    WHEN year BETWEEN 55 AND 64 THEN '55-64'
    WHEN year >=65 THEN '65+'
    ELSE NULL
  END AS fb_age_group
FROM include_100plus
),

uk_population_by_facebook_age_groups AS (
SELECT
  gender,
  fb_age_group,
  SAFE_CAST(ROUND(SUM(population_per_year)) AS INT64) AS uk_population
FROM add_facebook_age_groups
GROUP BY gender, fb_age_group
)
  

SELECT * 
FROM uk_population_by_facebook_age_groups
WHERE fb_age_group IS NOT NULL
"""
