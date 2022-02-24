{{ config(materialized='table') }}

WITH fhv_data AS (
    SELECT * 
    FROM {{ ref('stg_fhv_tripdata') }}
),

dim_zones AS (
    SELECT * 
    FROM {{ ref('dim_zones') }}
    WHERE Borough != 'Unknown'
)
SELECT 
    fhv_data.dispatching_base_num,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    fhv_data.PULocationID,
    pickup_zone.borough AS pickup_borough, 
    pickup_zone.zone AS pickup_zone,
    fhv_data.DOLocationID,
    dropoff_zone.borough AS dropoff_borough, 
    dropoff_zone.zone AS dropoff_zone,
    fhv_data.SR_Flag
FROM fhv_data
INNER JOIN dim_zones AS pickup_zone
ON fhv_data.PULocationID = pickup_zone.LocationID
INNER JOIN dim_zones AS dropoff_zone
ON fhv_data.DOLocationID = dropoff_zone.LocationID