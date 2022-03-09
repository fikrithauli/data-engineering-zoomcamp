{{ config(materialized='view') }}

WITH tripdata AS (
  SELECT *,
        ROW_NUMBER() OVER(PARTITION BY dispatching_base_num, pickup_datetime) AS rn
  FROM {{ source('staging', 'external_fhv_tripdata_partitioned_clustered') }}
  WHERE dispatching_base_num IS NOT NULL
)

SELECT 
    dispatching_base_num,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(PULocationID AS INTEGER) AS PULocationID,
    CAST(DOLocationID AS INTEGER) AS DOLocationID,
    CAST(SR_Flag AS INTEGER) AS SR_Flag
FROM tripdata
WHERE rn = 1

-- -- dbt build --m <model.sql> --var 'is_test_run: false'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}