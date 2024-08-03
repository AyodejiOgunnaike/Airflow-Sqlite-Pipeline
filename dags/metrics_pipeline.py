from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, Column, Interger, String, Float


# Define the SQLite databse connection
DB_PATH = 'C:/Users/Administrator/Desktop/Airflow_sqlite_pipeline/metrics.db'
engine = create_engine(f'sqlite:///{DB_PATH}', echo=True)
metadata = MetaData()

# Define the table schema
metrics_table = Table(
    'metrics',
    metadata,
    Column('month', String, primary_key=True),
    Column('sat_mean_trip_count, Float'),
    Column('sat_mean_fare_per_trip, Float'),
    Column('sat_mean_duration_per_trip, Float'),
    Column('sun_mean_trip_count, Float'),
    Column('sun_mean_fare_per_trip, Float'),
    Column('sun_mean_duration_per_trip, Float'),
)

# Define a function to fetch metrics and write to SQLite
def fetch_metrics_and_store():
    sql_script = """
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

"""

# Execute the query and store the results in SQLite
with engine.connect() as conn:
    conn.execute(metrics_table.delete()) # Clears previous data
    result = conn.execute(sql_script)
    for row in result:
        conn.execute(metrics_table.insert().values(
            month=row['month'],
            sat_mean_trip_count=row['sat_mean_trip_count'],
            sat_mean_fare_per_trip=row['sat_mean_fare_per_trip'],
            sat_mean_duration_per_trip=row['sat_mean_duration_per_trip'],
            sun_mean_trip_count=row['sun_mean_trip_count'],
            sun_mean_fare_per_trip=row['sun_mean_fare_per_trip'],
            sun_mean_duration_per_trip=row['sun_mean_duration_per_trip']
        ))

# DAG definition
my_dag = DAG(
    'metrics_pipeline',
    description='Fetch and store matrics in SQLite',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) 

# Declare task to fetch metrics and store in SQLite
fetch_metrics = PythonOperator(
    task_id='fetch_metrics_and_store',
    python_callable=fetch_metrics_and_store,
    dag=my_dag
)


# Set task dependecies
fetch_metrics






 