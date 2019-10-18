sql_demographics_table = """
SELECT 
  id AS ad_id, 
  demos.*
FROM `{staging_table_path}`
CROSS JOIN UNNEST(demographic_distribution) AS demos
"""

sql_demographics_impressions_spend_table = """
SELECT 
  id AS ad_id, 
  demos.*,
  impressions.upper_bound * demos.percentage AS impressions_upper_bound,
  (impressions.lower_bound + impressions.upper_bound / 2) * demos.percentage AS impressions_midpoint,
  impressions.lower_bound * demos.percentage AS impressions_lower_bound,
  spend.upper_bound * demos.percentage AS spend_upper_bound,
  (spend.lower_bound + spend.upper_bound / 2) * demos.percentage AS spend_midpoint,
  spend.lower_bound * demos.percentage AS spend_lower_bound,
  currency,
  funding_entity
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

sql_regions_impressions_spend_table = """
SELECT 
  id AS ad_id, 
  regions.*,
  impressions.upper_bound * regions.percentage AS impressions_upper_bound,
  (impressions.lower_bound + impressions.upper_bound / 2) * regions.percentage AS impressions_midpoint,
  impressions.lower_bound * regions.percentage AS impressions_lower_bound,
  spend.upper_bound * regions.percentage AS spend_upper_bound,
  (spend.lower_bound + spend.upper_bound / 2) * regions.percentage AS spend_midpoint,
  spend.lower_bound * regions.percentage AS spend_lower_bound,
  currency,
  funding_entity
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
SELECT COUNT(*)
FROM `{project}.{dataset}.{table}`
"""