DROP TABLE IF EXISTS agency;
CREATE TABLE agency (
	agency_id text,
    agency_name text,
    agency_url text,
    agency_timezone text,
    agency_lang text,
    agency_phone text,
    agency_fare_url text);
    
DROP TABLE IF EXISTS calendar;
CREATE TABLE calendar (
	service_id text,
    monday int,
    tuesday int,
    wednesday int,
    thursday int,
    friday int,
    saturday int,
    sunday int,
    start_date int,
    end_date int);
	
DROP TABLE IF EXISTS calendar_dates;
CREATE TABLE calendar_dates (
	service_id text,
    `date` int,
    exception_type int);
    
DROP TABLE IF EXISTS routes;
CREATE TABLE routes (
	route_id text,
    agency_id text,
    route_short_name text,
    route_long_name text,
    route_desc text,
    route_type text,
    route_url text,
    route_color text,
    route_text_color text);
    
DROP TABLE IF EXISTS shapes;
CREATE TABLE shapes (
	shape_id text,
    shape_pt_lat double,
    shape_pt_lon double,
    shape_pt_sequence int,
    shape_dist_traveled text);
    
DROP TABLE IF EXISTS stop_times;
CREATE TABLE stop_times (
	trip_id text,
    arrival_time time,
    departure_time time,
    stop_id text,
    stop_sequence int,
    stop_headsign text,
    pickup_type int,
    drop_off_type int,
    timepoint int);
    
DROP TABLE IF EXISTS stops;
CREATE TABLE stops (
	stop_id text,
    stop_code text,
    stop_name text,
    stop_desc text,
    stop_lat double,
    stop_lon double,
    zone_id text,
	stop_url text,
    location_type text,
    parent_station text,
    stop_timezone text,
    wheelchair_boarding text);
    
DROP TABLE IF EXISTS trips;
CREATE TABLE trips (
	route_id text,
    service_id text,
    trip_id text,
    trip_headsign text,
    trip_short_name text,
    direction_id int,
    block_id text,
    shape_id text,
    wheelchair_accessible text,
    bikes_allowed text);
    
#------------------------------------------

    
	

    

    
    
