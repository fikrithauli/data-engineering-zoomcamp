-- Create fhv_trip table from Google Cloud Storage
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata`
OPTIONS (
    format = 'parquet',
    uris = ['gs://dtc_data_lake_dtc-de-bootcamp/raw/fhv_tripdata_2019-*.parquet']
);

-- Create yellow_trip table from Google Cloud Storage
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-bootcamp.trips_data_all.external_yellow_tripdata`
OPTIONS (
    format = 'parquet',
    uris = ['gs://dtc_data_lake_dtc-de-bootcamp/raw/yellow_tripdata_2019-*.parquet', 'gs://dtc_data_lake_dtc-de-bootcamp/raw/yellow_tripdata_2020-*.parquet']
);

-- Create green_trip table from Google Cloud Storage
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-bootcamp.trips_data_all.external_green_tripdata`
OPTIONS (
    format = 'parquet',
    uris = ['gs://dtc_data_lake_dtc-de-bootcamp/raw/green_tripdata_2019-*.parquet', 'gs://dtc_data_lake_dtc-de-bootcamp/raw/green_tripdata_2020-*.parquet']
);

-- Create zonez_lookup table from Google Cloud Storage
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-bootcamp.trips_data_all.external_zones_lookup`
OPTIONS (
    format = 'parquet',
    uris = ['gs://dtc_data_lake_dtc-de-bootcamp/raw/zone_lookup.parquet']
);

-- Create non-partitioned fhv table 
CREATE OR REPLACE TABLE `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata_non-partitioned` AS
SELECT 
    *
FROM `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata`;

-- Create partitioned fhv table 
CREATE OR REPLACE TABLE `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata_partitioned`
PARTITION BY 
    DATE(dropoff_datetime) AS
SELECT 
    *
FROM `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata`;

-- Create partitioned & clustered fhv table 
CREATE OR REPLACE TABLE `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata_partitioned_clustered`
PARTITION BY 
    DATE(dropoff_datetime)
CLUSTER BY 
    dispatching_base_num AS
SELECT 
    *
FROM `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata`;