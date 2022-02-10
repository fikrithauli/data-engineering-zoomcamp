-- What is count for fhv vehicles data for year 2019? 42084899
SELECT
    COUNT(*)
FROM 
    `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata`;

-- How many distinct dispatching_base_num we have in fhv for 2019? 792
SELECT
    COUNT(DISTINCT dispatching_base_num)
FROM 
    `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata`;

-- What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279?
SELECT 
    COUNT(*)
FROM 
    `dtc-de-bootcamp.trips_data_all.external_fhv_tripdata_partitioned_clustered`
WHERE 
    DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
    AND
    dispatching_base_num IN ('B00987', 'B02060', 'B02279');