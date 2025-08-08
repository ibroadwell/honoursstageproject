DROP TABLE IF EXISTS stops_frequency;
CREATE TABLE stops_frequency AS
WITH TripsWithDayOfWeek AS (
    -- Create a simple mapping of every trip_id to a specific day of the week.
    SELECT trip_id, 'monday' AS day_of_week FROM trips t JOIN calendar c ON t.service_id = c.service_id WHERE c.monday = 1
    UNION ALL
    SELECT trip_id, 'tuesday' AS day_of_week FROM trips t JOIN calendar c ON t.service_id = c.service_id WHERE c.tuesday = 1
    UNION ALL
    SELECT trip_id, 'wednesday' AS day_of_week FROM trips t JOIN calendar c ON t.service_id = c.service_id WHERE c.wednesday = 1
    UNION ALL
    SELECT trip_id, 'thursday' AS day_of_week FROM trips t JOIN calendar c ON t.service_id = c.service_id WHERE c.thursday = 1
    UNION ALL
    SELECT trip_id, 'friday' AS day_of_week FROM trips t JOIN calendar c ON t.service_id = c.service_id WHERE c.friday = 1
    UNION ALL
    SELECT trip_id, 'saturday' AS day_of_week FROM trips t JOIN calendar c ON t.service_id = c.service_id WHERE c.saturday = 1
    UNION ALL
    SELECT trip_id, 'sunday' AS day_of_week FROM trips t JOIN calendar c ON t.service_id = c.service_id WHERE c.sunday = 1
),
DailyServiceSummary AS (
    -- For each stop and day, find the number of trips and the service window (min/max arrival times).
    SELECT
        st.stop_id,
        twdw.day_of_week,
        COUNT(st.arrival_time) AS num_trips,
        MIN(st.arrival_time) AS first_bus_arrival,
        MAX(st.arrival_time) AS last_bus_arrival
    FROM
        stop_times st
    JOIN
        TripsWithDayOfWeek twdw ON st.trip_id = twdw.trip_id
    GROUP BY
        st.stop_id,
        twdw.day_of_week
),
DailyCalculations AS (
    -- Calculate the daily frequency per hour
    SELECT
        stop_id,
        day_of_week,
        num_trips,
        -- Calculate the service window in seconds
        (TIME_TO_SEC(last_bus_arrival) - TIME_TO_SEC(first_bus_arrival)) AS service_window_seconds,
        
        -- Calculate the frequency, handling cases with one service
        CASE
            WHEN (TIME_TO_SEC(last_bus_arrival) - TIME_TO_SEC(first_bus_arrival)) = 0 THEN num_trips
            ELSE num_trips / ((TIME_TO_SEC(last_bus_arrival) - TIME_TO_SEC(first_bus_arrival)) / 3600.0)
        END AS daily_frequency_per_hour
    FROM
        DailyServiceSummary
)
-- Calculate the weekly average frequency for each stop
SELECT
    s.stop_id,
    AVG(dc.daily_frequency_per_hour) AS avg_weekly_frequency_per_hour
FROM
    stops s
LEFT JOIN
    DailyCalculations dc ON s.stop_id = dc.stop_id
GROUP BY
    s.stop_id
ORDER BY
    s.stop_id;

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
    si.population_density,
    t1.total AS oa21pop,
    pe.total AS `postcode_pop`,
    t61.travel_total_16_plus_employed AS `employed_total`,
    t61.travel_bus AS `bus_commute_total`,
    sf.avg_weekly_frequency_per_hour
FROM stops_intermediate AS si
LEFT JOIN stops_frequency AS sf ON sf.stop_id = si.stop_id
LEFT JOIN ts001 AS t1 ON t1.geography = si.oa21cd
LEFT JOIN ts007a AS t7a ON t7a.geography = si.oa21cd
LEFT JOIN ts061 AS t61 ON t61.geography = si.oa21cd
LEFT JOIN postcode_estimates AS pe ON pe.postcode = CASE WHEN LENGTH(si.postcode) > 7 THEN REPLACE(si.postcode, ' ', '') ELSE si.postcode END;

DROP TABLE IF EXISTS stops_intermediate;
DROP TABLE IF EXISTS stops_frequency;

ALTER TABLE stops_enriched ADD COLUMN customer_convenience_score DECIMAL(5, 4);
ALTER TABLE stops_enriched ADD COLUMN commute_opportunity_score DECIMAL(5, 4);

WITH MinMaxValues AS (
    SELECT
        MIN(LOG(1 + COALESCE(shops_nearby_count, 0))) AS min_log_shops,
        MAX(LOG(1 + COALESCE(shops_nearby_count, 0))) AS max_log_shops,
        MIN(LOG(1 + COALESCE(employed_total, 0))) AS min_log_employed,
        MAX(LOG(1 + COALESCE(employed_total, 0))) AS max_log_employed,
        MIN(LOG(1 + COALESCE(bus_commute_total, 0))) AS min_log_bus_commute,
        MAX(LOG(1 + COALESCE(bus_commute_total, 0))) AS max_log_bus_commute,
        MIN(LOG(1 + COALESCE(avg_weekly_frequency_per_hour, 0))) AS min_log_frequency,
        MAX(LOG(1 + COALESCE(avg_weekly_frequency_per_hour, 0))) AS max_log_frequency,
        MIN(LOG(1 + COALESCE(population_density, 0))) AS min_log_population_density,
        MAX(LOG(1 + COALESCE(population_density, 0))) AS max_log_population_density
    FROM
        stops_enriched
)
UPDATE stops_enriched se, MinMaxValues mmv
SET
    se.customer_convenience_score =
    CASE
        WHEN (CASE WHEN se.shops_nearby_count IS NOT NULL THEN 1 ELSE 0 END) +
             (CASE WHEN se.employed_total IS NOT NULL THEN 1 ELSE 0 END) +
             (CASE WHEN se.bus_commute_total IS NOT NULL THEN 1 ELSE 0 END) +
             (CASE WHEN se.avg_weekly_frequency_per_hour IS NOT NULL THEN 1 ELSE 0 END) +
             (CASE WHEN se.population_density IS NOT NULL THEN 1 ELSE 0 END) = 0
        THEN 0
        ELSE
            (
                CASE WHEN (mmv.max_log_shops - mmv.min_log_shops) = 0 THEN 0 ELSE (LOG(1 + COALESCE(se.shops_nearby_count, 0)) - mmv.min_log_shops) / (mmv.max_log_shops - mmv.min_log_shops) END +
                CASE WHEN (mmv.max_log_employed - mmv.min_log_employed) = 0 THEN 0 ELSE (LOG(1 + COALESCE(se.employed_total, 0)) - mmv.min_log_employed) / (mmv.max_log_employed - mmv.min_log_employed) END +
                CASE WHEN (mmv.max_log_bus_commute - mmv.min_log_bus_commute) = 0 THEN 0 ELSE (LOG(1 + COALESCE(se.bus_commute_total, 0)) - mmv.min_log_bus_commute) / (mmv.max_log_bus_commute - mmv.min_log_bus_commute) END +
                CASE WHEN (mmv.max_log_frequency - mmv.min_log_frequency) = 0 THEN 0 ELSE (LOG(1 + COALESCE(se.avg_weekly_frequency_per_hour, 0)) - mmv.min_log_frequency) / (mmv.max_log_frequency - mmv.min_log_frequency) END +
                CASE WHEN (mmv.max_log_population_density - mmv.min_log_population_density) = 0 THEN 0 ELSE (LOG(1 + COALESCE(se.population_density, 0)) - mmv.min_log_population_density) / (mmv.max_log_population_density - mmv.min_log_population_density) END
            ) /
            (
                (CASE WHEN (mmv.max_log_shops - mmv.min_log_shops) = 0 THEN 0 ELSE 1 END) +
                (CASE WHEN (mmv.max_log_employed - mmv.min_log_employed) = 0 THEN 0 ELSE 1 END) +
                (CASE WHEN (mmv.max_log_bus_commute - mmv.min_log_bus_commute) = 0 THEN 0 ELSE 1 END) +
                (CASE WHEN (mmv.max_log_frequency - mmv.min_log_frequency) = 0 THEN 0 ELSE 1 END) +
                (CASE WHEN (mmv.max_log_population_density - mmv.min_log_population_density) = 0 THEN 0 ELSE 1 END)
            )
    END,
    se.commute_opportunity_score =
    CASE
        WHEN (CASE WHEN se.employed_total IS NOT NULL THEN 1 ELSE 0 END) +
             (CASE WHEN se.bus_commute_total IS NOT NULL THEN 1 ELSE 0 END) = 0
        THEN 0
        ELSE
            (
                CASE WHEN (mmv.max_log_employed - mmv.min_log_employed) = 0 THEN 0 ELSE (LOG(1 + COALESCE(se.employed_total, 0)) - mmv.min_log_employed) / (mmv.max_log_employed - mmv.min_log_employed) END +
                CASE WHEN (mmv.max_log_bus_commute - mmv.min_log_bus_commute) = 0 THEN 0 ELSE (LOG(1 + COALESCE(se.bus_commute_total, 0)) - mmv.min_log_bus_commute) / (mmv.max_log_bus_commute - mmv.min_log_bus_commute) END
            ) /
            (
                (CASE WHEN (mmv.max_log_employed - mmv.min_log_employed) = 0 THEN 0 ELSE 1 END) +
                (CASE WHEN (mmv.max_log_bus_commute - mmv.min_log_bus_commute) = 0 THEN 0 ELSE 1 END)
            )
    END;
