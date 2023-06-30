# Final Project: Data Pipelines with Airflow
## Project Description
Sparkify, a music streaming company, has recognized the need to enhance automation and monitoring in their data warehouse ETL pipelines. After careful consideration, they have determined that Apache Airflow is the ideal tool to achieve these objectives.

As a member of the project team, Sparkify expects you to create high-quality data pipelines that are dynamic and constructed using reusable tasks. These pipelines should be easily monitored and capable of seamless backfills. Furthermore, Sparkify emphasizes the significance of data quality in facilitating accurate analyses on their data warehouse. To ensure this, they require tests to be conducted on the datasets after executing the ETL steps, thereby detecting any inconsistencies or discrepancies.

The source data for these pipelines is stored in Amazon S3, and it needs to be processed and loaded into Sparkify's data warehouse hosted on Amazon Redshift. The source datasets comprise JSON logs containing information about user activity within the application, as well as JSON metadata describing the songs listened to by the users.

## Datasets

- Log data: ```s3://udacity-dend/log_data```
- Song data: ```s3://udacity-dend/song_data```

## Implementation

## Prepare S3
- Create S3
```bash
aws s3 mb s3://airflow-final-project/
```

- Load data to local
```bash
aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
```

- Load data to S3
```bash
aws s3 cp ~/log-data/ s3://airflow-final-project/log-data/ --recursive
aws s3 cp ~/song-data/ s3://airflow-final-project/song-data/ --recursive
```

## Execution

1. Create S3 Bucket and copy data from source.
2. Add AWS connection Airflow via web UI
3. Create Redshift serverless
4. Run project DAG and monitor the execution via Airflow UI.