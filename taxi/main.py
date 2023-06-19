import time
from pathlib import Path

from common.spark import SparkSedonaFactory
from ingest.extract import TripExtractor
from ingest.load import TripLoader
from ingest.process import TripProcessor
from timescale.client import TimeScaleClient
from timescale.ddl import TripDatabase

if __name__ == "__main__":
    # Extract raw files
    TripExtractor().extract()

    # Initialize database schema
    # TripDatabase().setup()

    # Ingest normalized data to TimescaleDB
    raw = Path(__file__).parent / "data" / "raw"
    stage = Path(__file__).parent / "data" / "stage"
    spark = SparkSedonaFactory.session()
    processor = TripProcessor(spark)
    loader = TripLoader()

    for file in raw.glob("**/*.parquet"):
        print(file.as_posix())
        file_stage = stage / file.stem

        if not file_stage.exists():
            processor.transform(file, file_stage)
            time.sleep(5)

        files = [file.as_posix() for file in file_stage.glob("*.csv")]

        if len(files) > 0:
            print(file_stage.as_posix())
            loader.load(files)
