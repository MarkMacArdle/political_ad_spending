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
  'upper' AS bound,
  spend.upper_bound AS amount
FROM `{staging_table_path}`
UNION ALL
SELECT 
  id AS ad_id, 
  'lower' AS bound,
  spend.lower_bound AS amount
FROM `{staging_table_path}`
UNION ALL
SELECT 
  id AS ad_id, 
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
    impressions, 
    region_distribution, 
    demographic_distribution
  )
FROM `{staging_table_path}`
"""

sql_count_table_rows = """
SELECT COUNT({id_col})
FROM `{project}.{dataset}.{table}`
"""