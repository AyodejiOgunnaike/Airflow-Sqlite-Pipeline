WITH main AS (
    SELECT
        pickup_date,
        toDayOfWeek(pickup_date) AS pickup_day,
        COUNT(id) AS trip_count,
        AVG(fare_amount) AS fare_amount,
        AVG(dateDiff('second', pickup_datetime, dropoff_datetime)) / 60 AS trip_duration,
        toYYYYMM(pickup_date) AS month_of_year
    FROM tripdata
    WHERE pickup_date BETWEEN '2014-01-01' AND '2016-12-31'
    GROUP BY pickup_date, toDayOfWeek(pickup_date), toYYYYMM(pickup_date)
)
SELECT
    CONCAT(SUBSTRING(CAST(month_of_year AS String), 1, 4), '-', SUBSTRING(CAST(month_of_year AS String), 5, 2)) AS month,
    ROUND(AVG(CASE WHEN pickup_day = 6 THEN trip_count END), 1) AS sat_mean_trip_count,
    ROUND(AVG(CASE WHEN pickup_day = 6 THEN fare_amount END), 1) AS sat_mean_fare_per_trip,
    ROUND(AVG(CASE WHEN pickup_day = 6 THEN trip_duration END), 1) AS sat_mean_duration_per_trip,
    ROUND(AVG(CASE WHEN pickup_day = 7 THEN trip_count END), 1) AS sun_mean_trip_count,
    ROUND(AVG(CASE WHEN pickup_day = 7 THEN fare_amount END), 1) AS sun_mean_fare_per_trip,
    ROUND(AVG(CASE WHEN pickup_day = 7 THEN trip_duration END), 1) AS sun_mean_duration_per_trip
FROM main
GROUP BY month_of_year
ORDER BY month_of_year;
