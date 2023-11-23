# DataPipelineWeb-GCS-BQ

## Web to GCS to BigQuery Pipeline

# Overview

This project is designed to create an Extract, Transform, and Load (ETL) pipeline that retrieves NYC Green taxi data from the DataTalks GitHub Repo. The pipeline loads this data into a Google Cloud Storage (GCS) Bucket and transfers it from the GCS Bucket to a BigQuery Table. The entire pipeline is implemented using Apache Airflow with custom operators to facilitate the process.

## Data Architecture

<img width="499" alt="image" src="https://github.com/salmah52/DataPipelineWeb-GCS-BQ/assets/44398948/579d2b68-4472-4865-90e7-6cc132c4e945">

## Features

1. WebToGCSHKOperator: A custom Apache Airflow operator that downloads data from the specified web endpoint (DataTalks GitHub Repo) and uploads it to GCS.
2. GCSToBigQueryOperator: An Apache Airflow operator that loads data from GCS into BigQuery.

## Operators

# WebToGCSHKOperator
This operator downloads data from a web endpoint (DataTalks GitHub Repo) and uploads it to GCS.

Parameters
- **endpoint**: The URL of the web endpoint (DataTalks GitHub Repo).
- **destination_path**: The path to store the file in GCS.
- **destination_bucket**: The GCS bucket to upload the file.
- **service**: Service identifier for file naming.
- **gcp_conn_id**: The Airflow connection ID for GCP.
- **gzip**: Optional. Set to True if the file is GZipped.
- **mime_type**: Optional. The MIME type of the file.
- **delegete_to**: Optional. The service account to delegate the access to.
- **impersonation_chain**: Optional. The impersonation chain for service account.

# GCSToBigQueryOperator
This operator loads data from GCS into BigQuery.

Parameters
- bucket: The GCS bucket name.
- source_objects: The list of source objects (files) in GCS.
- destination_project_dataset_table: The destination table in BigQuery.
- autodetect: Optional. Set to True to automatically detect schema.
- write_disposition: Optional. Specify the write disposition.
- source_format: Optional. The format of the source data.

## Workflow
1. Download from Web to GCS: The WebToGCSHKOperator operator downloads data from the specified web endpoint (DataTalks GitHub Repo) and uploads it to the designated GCS bucket.
2. Load from GCS to BigQuery: The GCSToBigQueryOperator operator loads data from GCS into BigQuery.

## Conclusion
This pipeline simplifies the process of moving NYC Green taxi data from a web source (DataTalks GitHub Repo) to BigQuery, providing a flexible and scalable solution.
