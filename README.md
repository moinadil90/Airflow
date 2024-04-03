# Airflow
This is just a sample Airflow job, meant for learning. This solution leverages Airflow for orchestration, the google.cloud operator for interacting with BigQuery, and the redis-hook for interacting with Redis. The data will be extracted from BigQuery, transformed if necessary (considering RecSys), and loaded into Redis in a format suitable for Golang services.
