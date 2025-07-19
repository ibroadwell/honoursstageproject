USE hsp_eyms_enriched;

DROP TABLE IF EXISTS oa_lookup;
CREATE TABLE oa_lookup (
	pcd7 varchar(7),
    pcd8 varchar(8),
    pcds varchar(8), 
    dointr varchar(8),
    doterm varchar(8),
    usertype int,
    oa21cd varchar(9),
    lsoa21cd varchar(9),
    msoa21cd varchar(9),
    ladcd varchar(9),
    lsoa21nm varchar(65),
    msoa21nm varchar(65),
    ladnm varchar(45),
    ladnmw varchar(45));
    
