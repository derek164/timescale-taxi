# NYC "Yellow Taxi" Trips

## Questions
1. Return all the trips over 0.9 percentile in the distance traveled for any Parquet files you can find there.
2. Create a continuous aggregate that rolls up stats on passenger count and fare amount by pickup location.
3. Do not implement, but explain your architecture on how you’ll solve this problem. Another request we have is to upload that information to another system daily. We need a solution that allows us to push some information into our Salesforce so Sales, Finance, and Marketing can make better decisions. Some requirements to consider are:
    - We need to have a daily dump of the trips into Salesforce.
    - We should avoid duplicates and have a clear way to backfill if needed.

## Dependencies
- Timescale service (configured with `.yaml` under `taxi/timescale/databases`)
- Docker for environment with PySpark/Sedona installation and python libraries

## Usage
Build the image
```
make build
```
Start container
```
make start
```
Run the pipeline
```
make run
```
Preview database
```
make preview
```

## Database Schema
#### `location`
This table stores additional data about each taxi zone, sourced from the [Taxi Zone Lookup Table](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
```sql
CREATE TABLE IF NOT EXISTS location (
    LocationID INTEGER PRIMARY KEY,
    Borough VARCHAR(50),
    Zone VARCHAR(50),
    service_zone VARCHAR(50)
);
```

#### `trip`
This is the main table, representing trip data. It has foreign key constraints to the `location` table for pickup and dropoff locations. 
*Note*: For the most part, only the subset of columns needed to answer the questions for this assignment were used.
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

Since this table holds a large volume of time series data, I made it a [hypertable](https://docs.timescale.com/use-timescale/latest/hypertables/).
I also enabled [compression](https://docs.timescale.com/use-timescale/latest/compression/) to reduce storage size.
```sql
SELECT create_hypertable('trip', 'pickup_datetime', if_not_exists => TRUE);
SELECT remove_compression_policy('trip');
SELECT add_compression_policy('trip', INTERVAL '1d');
```

#### `pickup_location_daily_summary`
This view is a solution to question (2). It is a continuous aggregate of the `trip` table, with various statistics about passenger count and fare amount for a given pickup location and day.
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

#### `trip_distance_daily`
This view is a daily continuous aggregate that contains percentile aggregates for `trip_distance`. Using accessors, it becomes simple and fast to then calculate approximate percentiles over various time horizons.
```sql
CREATE MATERIALIZED VIEW trip_distance_daily
WITH (timescaledb.continuous)
AS SELECT
    time_bucket('1 day', pickup_datetime) AS day,
    tdigest(100, trip_distance) AS tdigest
FROM trip
GROUP BY day;
```

This was useful for question (3) when calculating the 0.9 percentile in the distance traveled across all trips, where you can roll up percentile data from a continuous aggregate as demonstrated in the Timescale [documentation](https://docs.timescale.com/api/latest/hyperfunctions/percentile-approximation/tdigest#extended-examples)
```sql
SELECT *
FROM trip
WHERE trip_distance > (SELECT approx_percentile(0.90, rollup(tdigest)) FROM trip_distance_daily);
```

## ETL Pipeline
With the database schema defined, the outstanding task was to build a pipeline that makes [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) available from our Timescale service so that we can answer out questions. I broke this down into a few steps:
1. Download the raw `.parquet` files.
2. Normalize the data to expected format.
3. Load the normalized data to Timescale.