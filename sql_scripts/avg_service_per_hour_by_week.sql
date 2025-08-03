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