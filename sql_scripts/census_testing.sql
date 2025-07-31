SELECT
	l.pcds as Postcode,
	t.geography as OA,
    t.travel_total_16_plus_employed as `Employed Pop`,
    t.travel_bus as `Travel to work with Bus`,
    CONCAT(ROUND((t.travel_bus / t.travel_total_16_plus_employed)*100, 2), "%") as `Travel to work with Bus %`
FROM oa_lookup as l
LEFT JOIN ts061 as t ON t.geography = l.oa21cd
WHERE pcds = "HU15 1SF"