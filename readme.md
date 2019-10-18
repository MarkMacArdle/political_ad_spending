## Set up needed for running stages of pipeline

### Running `ad_data_collector.py`

You'll need to get a user access token add it the config.py file

### Running `add_data_to_bigquery.py`

You'll need to:
 - Set up a service account on Google Cloud Platform
 - Give it permission to access BigQuery
 - Download the credentials json file
 - Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to be the 
 path to it
 
Further details [here](https://cloud.google.com/docs/authentication/getting-started) 
if needed.


# Data Sources

- Facebook Ad Library
- `guardian_facebook_users_2018.csv` is from 
this [Guardian article](https://www.theguardian.com/technology/2018/feb/12/is-facebook-for-old-people-over-55s-flock-in-as-the-young-leave) on 
Facebook users.
- `ons_uk_population_estimates_from_2016_for_2019.csv` is from the UK Office
 of National Statistics and available [here](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/tablea11principalprojectionuksummary).


