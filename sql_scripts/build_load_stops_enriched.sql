DROP TABLE IF EXISTS stops_enriched;
CREATE TABLE stops_enriched AS
SELECT
    si.stop_id,
    si.stop_name,
    si.stop_lat,
    si.stop_lon,
    CASE WHEN si.postcode = "" THEN NULL ELSE si.postcode END AS postcode,
    CASE WHEN si.oa21cd = "" THEN NULL ELSE si.oa21cd END AS oa21cd,
    CASE WHEN si.lsoa21cd = "" THEN NULL ELSE si.lsoa21cd END AS lsoa21cd,
    CASE WHEN si.lsoa21nm = "" THEN NULL ELSE si.lsoa21nm END AS lsoa21nm,
    si.shops_nearby_count,
    t1.total AS oa21pop,
    pe.total AS `postcode_pop`,
    t61.travel_total_16_plus_employed AS `employed_total`, -- Corrected alias
    t61.travel_bus AS `bus_commute_total` -- Corrected alias
FROM stops_intermediate AS si
LEFT JOIN ts001 AS t1 ON t1.geography = si.oa21cd
LEFT JOIN ts007a AS t7a ON t7a.geography = si.oa21cd
LEFT JOIN ts061 AS t61 ON t61.geography = si.oa21cd
LEFT JOIN postcode_estimates AS pe ON pe.postcode = CASE WHEN LENGTH(si.postcode) > 7 THEN REPLACE(si.postcode, ' ', '') ELSE si.postcode END;
DROP TABLE IF EXISTS stops_intermediate;