from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from pathlib import Path

import backoff
import pandas as pd
from func_timeout import FunctionTimedOut, func_timeout
from pgcopy import CopyManager
from timescale.client import TimeScaleClient

cols = (
    "trip_distance",
    "fare_amount",
    "passenger_count",
    "pulocationid",
    "dolocationid",
    "pickup_datetime",
    "dropoff_datetime",
)


class TripLoader:
    def __init__(self):
        self.timescale_db = TimeScaleClient(database="hosted")
        self.stage = Path(__file__).parent.parent / "data" / "stage"

    def load(self, files):
        with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
            futures = [executor.submit(self.copy, file) for file in files]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    print(result)

    @backoff.on_exception(
        backoff.constant,
        FunctionTimedOut,
        max_tries=3,
        interval=10,
        raise_on_giveup=False,
    )
    def copy(self, file: str, timeout=30):
        try:
            result = func_timeout(timeout, self.psql_copy_load, args=[file])
            return result
        except FunctionTimedOut as error:
            print(f"{file} timed out")
            raise error

    def psql_copy_load(self, file: str):
        values = self.read_partition(file)
        # print(values[0])

        conn = self.timescale_db.connection
        copy_mgr = CopyManager(conn, "trip", cols)
        copy_mgr.copy(values)
        conn.commit()

        Path(file).unlink()
        return file

    def read_partition(self, file: str):
        df = pd.read_csv(
            file,
            names=cols,
            delimiter=",",
            date_format="%Y-%m-%d %H:%M:%S",
            parse_dates=["pickup_datetime", "dropoff_datetime"],
        )
        df["pickup_datetime"] = pd.Series(
            df["pickup_datetime"].dt.to_pydatetime(), dtype="object"
        )
        df["dropoff_datetime"] = pd.Series(
            df["dropoff_datetime"].dt.to_pydatetime(), dtype="object"
        )
        return [tuple(row) for row in df.values]
