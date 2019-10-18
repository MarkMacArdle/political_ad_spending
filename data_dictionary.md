Lines are in the format: field_name, datatype, mode: description

# Ads

- id, STRING, NULLABLE, id code added during upload. Facebook don't give a 
designated id for an ad
- ad_creation_time, TIMESTAMP, NULLABLE: UTC timestamp of when ad was created
- ad_delivery_start_time, TIMESTAMP, NULLABLE: UTC timestamp of when ad starts
- ad_delivery_stop_time, TIMESTAMP, NULLABLE: UTC timestamp of when ad finished
- page_id, STRING, NULLABLE: page that ran the ad
- ad_creative_link_caption, STRING, NULLABLE: link in caption of ad
- funding_entity, STRING, NULLABLE: who paid for the ad	
- ad_creative_link_title, STRING, NULLABLE: title of the link if there was one
- ad_creative_body, STRING, NULLABLE: text of body of ad
- ad_creative_link_description, STRING, NULLABLE: description link if there 
was one


# Demographics

- ad_id, STRING, NULLABLE: id of ad, foreign key from `ads` table
- gender, STRING, NULLABLE: male, female or unknown
- age, STRING, NULLABLE: age ranges ad was shown to
- percentage, FLOAT, NULLABLE: percentage of impressions for ad shown to 
each gender and age range combination


# Impressions

- ad_id, STRING, NULLABLE: id of ad, foreign key from `ads` table
- bound, STRING, NULLABLE: what boundary the amount represents (upper, lower
 or midpoint)
- amount, FLOAT, NULLABLE: amount of impressions


# Regions

- ad_id, STRING, NULLABLE: id of ad, foreign key from `ads` table
- region, STRING, NULLABLE: region ad was aimed at (England, Scotland, Wales
 or Northern Ireland
- percentage, FLOAT, NULLABLE: percentage of impressions from each region

# Spends

- ad_id, STRING, NULLABLE: id of ad, foreign key from `ads` table
- currency, STRING, NULLABLE: currency ad was paid for in
- bound, STRING, NULLABLE: what boundary the amount represents (upper, lower
 or midpoint)
- amount, FLOAT, NULLABLE: amount spent


# facebook_users_by_age_groups

Data taken from [this](https://www.theguardian.com/technology/2018/feb/12/is-facebook-for-old-people-over-55s-flock-in-as-the-young-leave) Guardian article

- age_group, STRING, NULLABLE: age band population applies to
- population, INTEGER, NULLABLE: count of users for age band


# uk_population_by_age_groups

Data taken from the Office of National Statistics [website]()

- gender, STRING, NULLABLE: male or female
- fb_age_group, STRING, NULLABLE: age group matching the ones Facebook uses
- uk_population, INTEGER, NULLABLE: uk population in that age group and gender
