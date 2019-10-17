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

