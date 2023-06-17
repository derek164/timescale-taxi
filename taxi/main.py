import time
from pathlib import Path

from common.spark import SparkSedonaFactory
from ingest.extract import TripExtractor
from ingest.load import TripLoader
from ingest.process import TripProcessor
from timescale.client import TimeScaleClient
from timescale.ddl import TripDatabase

if __name__ == "__main__":
    """EXTRACT"""
    # TripExtractor().extract()

    """DDL"""
    trip_db = TripDatabase()
    # trip_db.setup()
    trip_db.decompress_trip_hypertable()
    trip_db.preview_location_table()
    trip_db.preview_trip_table()
    trip_db.preview_pickup_location_daily_summary_view()

    """INGEST"""
    # raw = Path(__file__).parent / "data" / "raw"
    # stage = Path(__file__).parent / "data" / "stage"
    # spark = SparkSedonaFactory.session()
    # processor = TripProcessor(spark)
    # loader = TripLoader()

    # for file in raw.glob("**/*.parquet"):
    #     print(file.as_posix())
    #     file_stage = stage / file.stem

    #     if not file_stage.exists():
    #         processor.transform(file, file_stage)
    #         time.sleep(5)

    #     files = [file.as_posix() for file in file_stage.glob("*.csv")]

    #     if len(files) > 0:
    #         print(file_stage.as_posix())
    #         loader.load(files)

        # break


"""TEST"""
# processor = TripProcessor(spark)
# df = processor.transform(Path("/app/taxi/data/raw/Pre2011/yellow_tripdata_2009-08.parquet"))
# df = processor.transform(Path("/app/taxi/data/raw/Post2011/yellow_tripdata_2011-08.parquet"))


# (datetime.datetime(2023, 6, 16, 0, 29, 28, 39954, tzinfo=datetime.timezone.utc), 1, 53.83868502482627, 0.9917763836013929)


# from tqdm import tqdm
# import requests

# file = "yellow_tripdata_2023-01.parquet"
# url = "https://d37ci6vzurychx.cloudfront.net/trip-data/" + file
# response = requests.get(url, stream=True)
# with open(file, "wb") as handle:
#     for data in tqdm(response.iter_content(chunk_size=1000000), unit='MB', desc=file):
#         handle.write(data)
