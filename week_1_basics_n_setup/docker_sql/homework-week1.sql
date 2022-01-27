-- 3. Count records
-- How many taxi trips were there on January 15?
-- SQL Query: 

SELECT 
	COUNT(1)
FROM 
	yellow_taxi_trips
WHERE
	to_char(tpep_pickup_datetime, 'YYYY-MM-DD') = '2021-01-15';

-- Answer: 53024


-- 4. Largest tip for each day *
-- On which day it was the largest tip in January? (note: it's not a typo, it's "tip", not "trip")
-- SQL Query:

SELECT 
	TO_CHAR(tpep_pickup_datetime, 'YYYY-MM-DD') date,
	tip_amount
FROM 
	yellow_taxi_trips
WHERE
	tip_amount = (SELECT MAX(tip_amount) FROM yellow_taxi_trips);

-- Answer: 2021-01-20 | 1140.44


-- 5. Most popular destination *
-- What was the most popular destination for passengers picked up in central park on January 14? Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"
-- SQL Query:

SELECT
	COALESCE(zdo."Zone", 'Unknown') destination,
	COUNT(1) count_destination
FROM 
	yellow_taxi_trips t
INNER JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
INNER JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE 
	TO_CHAR(tpep_pickup_datetime, 'YYYY-MM-DD') = '2021-01-14'
	AND
	zpu."Zone" = 'Central Park'
GROUP BY 1
ORDER BY COUNT(1) DESC;

-- Answer: Upper East Side South | 97

-- 6. Most expensive route *
-- What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)? Enter two zone names separated by a slashFor example:"Jamaica Bay / Clinton East"If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".
-- SQL Query:

SELECT
	CONCAT(COALESCE(zpu."Zone", 'Unknown'), ' / ', COALESCE(zdo."Zone", 'Unknown')) pickup_dropoff,
	AVG(t.total_amount) avg_price
FROM 
	yellow_taxi_trips t
INNER JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
INNER JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
GROUP BY 1
ORDER BY 2 DESC;

-- Answer: Alphabet City / Unknown