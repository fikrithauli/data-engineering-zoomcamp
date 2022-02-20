{{ config(materialized='table') }}

SELECT
    LocationID,
    Borough,
    Zone,
    REPLACE(service_zone, 'Boro', 'Green') AS service_zone
FROM {{ ref('taxi_zone_lookup') }}

