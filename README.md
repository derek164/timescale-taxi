# NYC "Yellow Taxi" Trips

## Dependencies
- Timescale Service (configured with `.yaml` under `taxi/timescale/databases`)
- Docker image containing PySpark/Sedona installation and python libraries

## Setup
Build the image
```
make build
```
Start a container
```
make start
```
Run the process
```
make run
```

## Database Schema
- location
```sql
CREATE TABLE IF NOT EXISTS location (
    LocationID INTEGER PRIMARY KEY,
    Borough VARCHAR(50),
    Zone VARCHAR(50),
    service_zone VARCHAR(50)
);
```
- trip
```sql
CREATE TABLE IF NOT EXISTS trip (
    trip_distance DOUBLE PRECISION  NULL,
    fare_amount DOUBLE PRECISION    NULL,
    passenger_count INTEGER         NULL,
    PULocationID INTEGER            NULL,
    DOLocationID INTEGER            NULL,
    pickup_datetime TIMESTAMP       NOT NULL,
    dropoff_datetime TIMESTAMP      NOT NULL,
    FOREIGN KEY (PULocationID) REFERENCES location (LocationID),
    FOREIGN KEY (DOLocationID) REFERENCES location (LocationID)
);
```
```sql
SELECT create_hypertable('trip', 'pickup_datetime', if_not_exists => TRUE);
SELECT remove_compression_policy('trip');
SELECT add_compression_policy('trip', INTERVAL '1d');
```
- pickup_location_daily_summary
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS pickup_location_daily_summary
WITH (timescaledb.continuous) 
AS SELECT
    PULocationID,
    time_bucket('1 day', pickup_datetime) AS day,
    avg(passenger_count) AS avg_passenger_count,
    min(passenger_count) AS min_passenger_count,
    max(passenger_count) AS max_passenger_count,
    avg(fare_amount) AS avg_fare_amount,
    min(fare_amount) AS min_fare_amount,
    max(fare_amount) AS max_fare_amount,
    count(*) AS num_trips
FROM trip
GROUP BY day, PULocationID;
```
- trip_distance_daily
```sql
CREATE MATERIALIZED VIEW trip_distance_daily
WITH (timescaledb.continuous)
AS SELECT
    time_bucket('1 day', pickup_datetime) AS day,
    tdigest(100, trip_distance) AS tdigest
FROM trip
GROUP BY day;
```

## Questions
1. Return all the trips over 0.9 percentile in the distance traveled for any Parquet files you can find there.
2. Create a continuous aggregate that rolls up stats on passenger count and fare amount by pickup location.
3. Do not implement, but explain your architecture on how you’ll solve this problem. Another request we have is to upload that information to another system daily. We need a solution that allows us to push some information into our Salesforce so Sales, Finance, and Marketing can make better decisions. Some requirements to consider are:
    - We need to have a daily dump of the trips into Salesforce.
    - We should avoid duplicates and have a clear way to backfill if needed.