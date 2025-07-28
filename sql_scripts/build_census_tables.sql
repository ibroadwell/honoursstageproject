DROP TABLE IF EXISTS ts001;
CREATE TABLE ts001 (
	`date` int,
    geography varchar(9),
    geography_code varchar(9),
    total int,
    household int,
    communal int
    ); -- optionally enclosed by '"'
    
DROP TABLE IF EXISTS ts007a;
CREATE TABLE ts007a (
	`date` int,
    geography varchar(9),
    geography_code varchar(9),
    age_total int,
    age_0_4 int,
    age_5_9 int,
    age_10_14 int,
    age_15_19 int,
    age_20_24 int,
    age_25_29 int,
    age_30_34 int,
    age_35_39 int,
    age_40_44 int,
    age_45_49 int,
    age_50_54 int,
    age_55_59 int,
    age_60_64 int,
    age_65_69 int,
    age_70_74 int,
    age_75_79 int,
    age_80_84 int,
    age_85_plus int
);

DROP TABLE IF EXISTS ts061;
CREATE TABLE ts061 (
	`date` int,
    geography varchar(9),
    geography_code varchar(9),
    travel_total_16_plus_employed int,
    travel_remote int,
    travel_metro int,
    travel_train int,
    travel_bus int,
    travel_taxi int,
    travel_motorbike int,
    travel_car int,
    travel_car_passenger int,
    travel_bike int,
    travel_foot int,
    travel_other int
);

DROP TABLE IF EXISTS ts062;
CREATE TABLE ts062 (
	`date` int,
    geography varchar(9),
    geography_code varchar(9),
    nssec_total_16_plus int,
    nssec_l1_l3_higher_mgr int,
    nssec_l4_l6_lower_mgr int,
    nssec_l7_intermediate int,
    nssec_l8_l9_small_employer int,
    nssec_l10_l11_lower_sup int,
    nssec_l12_semi_routine int,
    nssec_l13_routine int,
    nssec_l14_never_work_long_unemp int,
    nssec_l15_students int
);