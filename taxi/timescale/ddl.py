from pathlib import Path

import pandas as pd
from pgcopy import CopyManager
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from timescale.client import TimeScaleClient


pd.set_option("display.width", 120)
pd.set_option("display.max_columns", 7)


class TripDatabase:
    def __init__(self):
        self.timescale_db = TimeScaleClient(database="hosted")

    def setup(self):
        with self.timescale_db.connection as conn:
            cursor = conn.cursor()
            cursor.execute(self.create_location_table())
            cursor.execute(self.populate_location_table())
            cursor.execute(self.create_trip_table())
            cursor.execute(self.create_trip_hypertable())
            conn.commit()

        self.create_trip_distance_index()
        self.enable_trip_hypertable_compression()
        self.create_pickup_location_daily_summary_view()
        self.create_trip_distance_daily_view()

    def create_location_table(self):
        return """
        CREATE TABLE IF NOT EXISTS location (
            LocationID INTEGER PRIMARY KEY,
            Borough VARCHAR(50),
            Zone VARCHAR(50),
            service_zone VARCHAR(50)
        );
        """

    def populate_location_table(self):
        source_file = Path(__file__).parent.parent.joinpath(
            "data/taxi_zones/taxi_zone_lookup.csv"
        )
        cols = ("locationid", "borough", "zone", "service_zone")
        df = pd.read_csv(source_file, delimiter=",", names=cols, header=0)
        # print(df.to_string())
        timescale_db = TimeScaleClient(database="hosted")
        values = [tuple(row) for row in df.values]
        conn = timescale_db.connection
        copy_mgr = CopyManager(conn, "location", cols)
        copy_mgr.copy(values)
        conn.commit()

    def create_trip_table(self):
        return """
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
        """

    def create_trip_hypertable(self):
        return "SELECT create_hypertable('trip', 'pickup_datetime', if_not_exists => TRUE);"

    def drop_trip_table(self):
        with self.timescale_db.connection as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE trip;")

    def enable_trip_hypertable_compression(self):
        conn = self.timescale_db.connection
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(
            """
            ALTER TABLE trip SET (
                timescaledb.compress,
                timescaledb.compress_orderby = 'pickup_datetime DESC, dropoff_datetime DESC',
                timescaledb.compress_segmentby = 'pulocationid, dolocationid'
            );
            """
        )
        conn.commit()
        conn.close()

    def create_pickup_location_daily_summary_view(self):
        conn = self.timescale_db.connection
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(
            """
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
            """
        )
        conn.commit()
        conn.close()

    def create_trip_distance_daily_view(self):
        conn = self.timescale_db.connection
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE MATERIALIZED VIEW trip_distance_daily
            WITH (timescaledb.continuous)
            AS SELECT
                time_bucket('1 day', pickup_datetime) AS day,
                tdigest(120, trip_distance) AS tdigest
            FROM trip
            GROUP BY day;
            """
        )
        conn.commit()
        conn.close()

    def preview(self):
        print(" location table ".center(120, "-"))
        self.preview_location_table()
        print(" trip table ".center(120, "-"))
        self.preview_trip_table()
        print(" trips in top 10th percentile for distance ".center(120, "-"))
        self.preview_top10_distance_trips()
        print(" daily summary stats by pickup location ".center(120, "-"))
        self.preview_pickup_location_daily_summary_view()

    def preview_location_table(self):
        with self.timescale_db.connection as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM location LIMIT 5;")
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            print(df.head())

    def preview_trip_table(self):
        with self.timescale_db.connection as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM trip LIMIT 5;")
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            print(df.head())

    def preview_pickup_location_daily_summary_view(self):
        with self.timescale_db.connection as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pickup_location_daily_summary LIMIT 5;")
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            print(df.head())

    def preview_top10_distance_trips(self):
        with self.timescale_db.connection as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM trip
                WHERE trip_distance > (SELECT approx_percentile(0.90, rollup(tdigest)) FROM trip_distance_daily)
                LIMIT 5;
                """
            )
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(cursor.fetchall(), columns=columns)
            print(df.head())
