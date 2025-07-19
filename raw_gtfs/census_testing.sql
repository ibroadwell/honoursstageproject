SELECT
	l.pcds,
	t.geography,
    CONCAT(ROUND((t.travel_car / t.travel_total_16_plus_employed)*100, 2), "%") as `Travel %`
FROM hsp_eyms_enriched.oa_lookup as l
LEFT JOIN census2021.ts061 as t ON t.geography = l.oa21cd
WHERE pcds = "HU15 1SF"