-- SQLite
CREATE TABLE metrics ( 
  month TEXT PRIMARY KEY,
  sat_mean_trip_count REAL,
  sat_mean_fare_per_trip REAL,
  sat_mean_duration_per_trip REAL,
  sun_mean_trip_count REAL,
  sun_mean_fare_per_trip REAL,
  sun_mean_duration_per_trip REAL ); 