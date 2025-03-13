-- Install and load the spatial extension
INSTALL spatial;
LOAD spatial;

-- Create Airbnb and Yelp tables
CREATE TABLE airbnb AS 
SELECT *, ST_Point(longitude, latitude) AS geom 
FROM read_csv_auto('team21/output/airbnb_cleaned.csv');

CREATE TABLE yelp AS 
SELECT *, ST_Point(longitude, latitude) AS geom 
FROM read_csv_auto('team21/output/yelp_cleaned.csv');

-- Compute distances between all Airbnb and Yelp restaurants
COPY (
    SELECT 
        a.id AS airbnb_id, 
        y.business_id AS yelp_id, 
        ST_Distance(a.geom, y.geom) * 100 AS distance_km
    FROM airbnb a
    JOIN yelp y
) TO 'team21/output/airbnb_yelp_distances.csv' WITH (HEADER, DELIMITER ',');
