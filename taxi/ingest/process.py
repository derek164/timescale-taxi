from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.utils.adapter import Adapter
from timescale.client import TimeScaleClient


class TripProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.read_taxi_zones()
        self.timescale_db = TimeScaleClient(database="hosted")
        self.stage = Path(__file__).parent.parent / "data" / "stage"

    def transform(self, file: Path, file_stage: Path):
        subdir = file.parts[-2]
        transformer = {
            "Pre2011": Pre2011Transformer,
            "Post2011": Post2011Transformer,
        }.get(subdir)
        df = self.validate(transformer(self.spark).transform(file))
        self.write(df, file_stage)

    def validate(self, df):
        valid_schema = {
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("DOLocationID", IntegerType(), True),
            StructField("pickup_datetime", StringType(), True),
            StructField("dropoff_datetime", StringType(), True),
        }
        diff = StructType(valid_schema.difference(set(df.schema)))
        if not diff:
            return df
        else:
            raise SystemExit(
                """Invalid output schema.\nThe following fields:\n{diff}\nare missing from\n{out}
                """.format(
                    diff=diff.simpleString(), out=df.schema.simpleString()
                )
            )

    def write(self, df, file_stage):
        # print(file_stage.as_posix())
        (
            df.repartition(cpu_count())
            .write.option("maxRecordsPerFile", 100000)
            .mode("overwrite")
            .csv(file_stage.as_posix())
        )

    def read_taxi_zones(self):
        """
        Source CRS: NAD83 / New York Long Island (https://epsg.io/2263)
        Target CRS: World Geodetic System 1984 (https://epsg.io/4326)
        """

        taxi_zone_lookup = self.spark.read.csv(
            "taxi/data/taxi_zones/taxi_zone_lookup.csv",
            header=True,
        )
        taxi_zone_lookup.createOrReplaceTempView("taxi_zone_lookup")
        # taxi_zone_lookup.printSchema()
        # print(taxi_zone_lookup.limit(5).toPandas())

        taxi_zones_shape = ShapefileReader.readToGeometryRDD(
            self.spark.sparkContext, "taxi/data/taxi_zones/shape/"
        )
        taxi_zones_geom = Adapter.toDf(taxi_zones_shape, self.spark)
        taxi_zones_geom.createOrReplaceTempView("taxi_zones_geom")
        # taxi_zones_geom.printSchema()
        # print(taxi_zones_geom.limit(5).toPandas())

        taxi_zones = self.spark.sql(
            """
            SELECT
                lookup.*,
                ST_FlipCoordinates(
                    ST_Transform(
                        ST_GeomFromWKT(CAST(zone.geometry AS string)),
                        'epsg:2263',
                        'epsg:4326',
                        true
                    )
                ) as geometry
            FROM
                taxi_zones_geom zone
                LEFT JOIN taxi_zone_lookup lookup ON zone.LocationID = lookup.LocationID
            WHERE
                service_zone = 'Yellow Zone'
            """
        )
        taxi_zones.createOrReplaceTempView("taxi_zones")
        # taxi_zones.printSchema()
        # print(taxi_zones.limit(5).toPandas())


class Post2011Transformer:
    def __init__(self, spark):
        self.spark = spark

    def transform(self, file: Path):
        raw_post2011_trips = self.spark.read.parquet(file.as_posix())
        # raw_post2011_trips = raw_post2011_trips.limit(100)  # testing
        raw_post2011_trips.createOrReplaceTempView("raw_post2011_trips")
        post2011_trips = self.spark.sql(
            """
            SELECT t.Trip_distance AS trip_distance,
                t.fare_amount,
                CAST(t.passenger_count AS integer) AS passenger_count,
                CAST(t.PULocationID AS integer) AS PULocationID,
                CAST(t.DOLocationID AS integer) AS DOLocationID,
                date_format(t.tpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss') AS pickup_datetime,
                date_format(t.tpep_dropoff_datetime,'yyyy-MM-dd HH:mm:ss') AS dropoff_datetime
            FROM raw_post2011_trips t 
            WHERE t.PULocationID <= 263 
                AND t.DOLocationID <= 263
                AND t.tpep_pickup_datetime >= '2009-01-01'
                AND t.tpep_pickup_datetime <= '2023-03-31'
            ORDER BY t.tpep_pickup_datetime
            """
        )
        post2011_trips.printSchema()
        # print(post2011_trips.limit(5).toPandas())
        post2011_trips.show(5)
        return post2011_trips


class Pre2011Transformer:
    def __init__(self, spark):
        self.spark = spark

    def transform(self, file: Path):
        year = datetime.strptime(file.stem.split("_")[-1], "%Y-%m").year
        self.read_pre2011_trips(file.as_posix(), year)
        return self.trip_zone_spatial_join(year)

    def read_pre2011_trips(self, file_path, year):
        pre2011_trips = self.spark.read.parquet(file_path)
        # pre2011_trips = pre2011_trips.limit(100)  # testing
        pre2011_trips.createOrReplaceTempView("pre2011_trips")

        if year == 2009:
            query = """
            SELECT *,
                ST_Point(Start_Lon, Start_Lat) AS PU_geometry,
                ST_Point(End_Lon, End_Lat) AS DO_geometry
            FROM pre2011_trips
            """

        if year == 2010:
            query = """
            SELECT *,
                ST_Point(pickup_longitude, pickup_latitude) AS PU_geometry,
                ST_Point(dropoff_longitude, dropoff_latitude) AS DO_geometry
            FROM pre2011_trips
            """

        pre2011_trips_with_geom = self.spark.sql(query)
        pre2011_trips_with_geom.createOrReplaceTempView("pre2011_trips_with_geom")
        # pre2011_trips_with_geom.printSchema()
        # print(pre2011_trips_with_geom.limit(5).toPandas())

    def trip_zone_spatial_join(self, year):
        if year == 2009:
            query = """
            SELECT t.Trip_distance AS trip_distance,
                t.Fare_Amt AS fare_amount,
                CAST(t.Passenger_Count AS integer) AS passenger_count,
                CAST(PU_zone.LocationID AS integer) AS PULocationID,
                CAST(DO_zone.LocationID AS integer) AS DOLocationID,
                date_format(t.Trip_Pickup_DateTime,'yyyy-MM-dd HH:mm:ss') AS pickup_datetime,
                date_format(t.Trip_Dropoff_DateTime,'yyyy-MM-dd HH:mm:ss') AS dropoff_datetime
            FROM pre2011_trips_with_geom t
            INNER JOIN taxi_zones AS PU_zone ON ST_Intersects(PU_zone.geometry, t.PU_geometry)
            INNER JOIN taxi_zones AS DO_zone ON ST_Intersects(DO_zone.geometry, t.DO_geometry)
            WHERE t.Trip_Pickup_DateTime >= '2009-01-01'
                AND t.Trip_Pickup_DateTime <= '2023-03-31'
            ORDER BY t.Trip_Pickup_DateTime
            """

        if year == 2010:
            query = """
            SELECT t.trip_distance,
                t.fare_amount,
                CAST(t.passenger_count AS integer) AS passenger_count,
                CAST(PU_zone.LocationID AS integer) AS PULocationID,
                CAST(DO_zone.LocationID AS integer) AS DOLocationID,
                date_format(t.pickup_datetime,'yyyy-MM-dd HH:mm:ss') AS pickup_datetime,
                date_format(t.dropoff_datetime,'yyyy-MM-dd HH:mm:ss') AS dropoff_datetime
            FROM pre2011_trips_with_geom t
            INNER JOIN taxi_zones AS PU_zone ON ST_Intersects(PU_zone.geometry, t.PU_geometry)
            INNER JOIN taxi_zones AS DO_zone ON ST_Intersects(DO_zone.geometry, t.DO_geometry)
            WHERE t.Trip_Pickup_DateTime >= '2009-01-01'
                AND t.Trip_Pickup_DateTime <= '2023-03-31'
            ORDER BY t.pickup_datetime
            """

        pre2011_trips_with_PULocationID = self.spark.sql(query)
        # pre2011_trips_with_PULocationID.createOrReplaceTempView(
        #     "pre2011_trips_with_PULocationID"
        # )
        pre2011_trips_with_PULocationID.printSchema()
        # print(pre2011_trips_with_PULocationID.limit(5).toPandas())
        # pre2011_trips_with_PULocationID.show(5)
        return pre2011_trips_with_PULocationID
