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
Run the pipeline (skip since data is already in Timescale)
```
make run
```
Preview answers
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
***Note***: For the most part, only the subset of columns needed to answer the questions for this assignment were used.
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
With the database schema defined, the outstanding task was to build a pipeline that makes [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) available from our Timescale service so that we can answer our questions.  

</br>

I broke this down into a few steps:
1. Download the raw `.parquet` files.
    > Used multiprocessing to parallelize the downloads. Created separate directories for pre- and post-2011 files to accomodate for different schemas. Pre-2011 files contained pickup and dropoff coordinates rather than Taxi Zone IDs, which required extra processing.
2. Normalize and stage the data.
    > In this Timescale [tutorial](https://docs.timescale.com/tutorials/latest/nyc-taxi-cab/advanced-nyc/), I learned that you could combine the data in the NYC taxi dataset with geospatial data using the `PostGIS` extension. However, I think it makes more sense to pre-process the data so that it is faster and simpler to draw insights in Timescale. I used `spark sql` and `sedona` to get Taxi Zone IDs from coordinates for Pre-2011 files. Additionally, I selected the desired columns, standardized column names, and normalized data types before validating the processed dataframe against the expected schema. As a final step, I partitioned the dataframe and wrote it out to `.csv` files with no more than 100,000 records.
3. Load the staged data to Timescale.
    > Used multiprocessing in combination with [pgcopy](https://pgcopy.readthedocs.io/en/1.5.0/) for fast data loading into Timescale with [binary copy](https://www.postgresql.org/docs/9.3/sql-copy.html). Each staged file is deleted after successful ingestion to Timescale.

As such, the structure of the file store is as follows:
```
taxi
│
└───data
    │
    └───raw
    │   │
    │   └───Pre2011
    │   │   │   yellow_tripdata_2009-01.parquet
    │   │   │   ...
    │   │
    │   └───Post2011
    │       │   yellow_tripdata_2011-01.parquet
    │       │   ...
    │
    └───stage
        │
        └───yellow_tripdata_YYYY-MM
            │   _SUCCESS
            │   part-00000-30702540-01d8-4b6d-a5a6-986f6f7dcaeb-c000.csv
            │   part-00000-30702540-01d8-4b6d-a5a6-986f6f7dcaeb-c001.csv
            │   ...
```

In a production setting, I would have used `S3` to store the raw files and `HDFS` as a file store for the processing and ingestion steps. 

## Daily Saleforce Load
Before getting into the architecture, we need to make a fanciful assumption that the source data is updated on a daily basis. In actuality, trip data is published monthly (with two months delay) instead of bi-annually as of 05/13/2022. Additionally, we'll assume that the source data format is comparable to what it is now.

</br>

With that said, this pipeline could follow this general strategy:
1. In the process that populates Timescale with trip data, include a field in the `trip` table to denote the time at which a record was inserted.
2. Trigger an overnight process to migrate trips inserted the previous day to Salesforce.

</br>

With respect to architecture, there are a lot of options, but here's what I might implement:
1. Scheduled cloudwatch event to trigger a lambda function.
2. Lambda function trigger glue job, and passes on date parameter from event.
3. Glue job to extract records inserted on the given date from Timescale and write to Salesforce.
    - Use spark connectors to interface with Timescale and Salesforce.
    - The default date is the previous day. For backfills, specify the date in the cloudwatch event.
