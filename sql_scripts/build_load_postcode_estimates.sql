DROP TABLE IF EXISTS postcode_estimates;
CREATE TABLE postcode_estimates (
	postcode varchar(8),
    total int,
    males int,
    females int,
    households int);
    
LOAD DATA INFILE "{SECURE_PRIV_PATH}/census_Postcode_Estimates_Table_1.csv"
INTO TABLE postcode_estimates
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;