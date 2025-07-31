DROP TABLE IF EXISTS stops_intermediate;
CREATE TABLE stops_intermediate (
	stop_id text,
    stop_name text,
    stop_lon double,
    stop_lat double,
	postcode text,
    oa21cd text,
    lsoa21cd text, 
    lsoa21nm text, 
    shops_nearby_count int);
    
LOAD DATA INFILE '{SECURE_PRIV_PATH}/stops_intermediate.csv' 
INTO TABLE stops_intermediate
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;