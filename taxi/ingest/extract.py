from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
from pathlib import Path

import backoff
import pandas as pd
import requests
from requests.exceptions import RequestException
from tqdm import tqdm


class TripExtractor:
    def __init__(self):
        self.raw = Path(__file__).parent.parent / "data" / "raw"

    def extract(self):
        with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
            urls, years = self._generate_download_params()
            executor.map(self._extract_file, urls, years)

    def _extract_file(self, url, year):
        file_name = url.split("/")[-1]
        subdir = self._get_subdir(year)
        target_dir = self.raw / subdir
        target_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = target_dir / file_name

        if not parquet_path.exists():
            url = self._validate_url(url)
            if url:
                self._download_file(url, parquet_path)

    def _get_subdir(self, year):
        return "Pre2011" if year < 2011 else "Post2011"

    @backoff.on_exception(backoff.expo, RequestException, max_tries=5)
    def _validate_url(self, url):
        response = requests.head(url, allow_redirects=True)
        response.raise_for_status()
        if response.status_code == 200:
            return url

    @backoff.on_exception(backoff.expo, RequestException, max_tries=5)
    def _download_file(self, url, parquet_path):
        response = requests.get(url, stream=True)
        with open(parquet_path, "wb") as handle:
            for data in tqdm(
                response.iter_content(chunk_size=1000000),
                unit="MB",
                desc=parquet_path.name,
            ):
                handle.write(data)

    def _generate_download_params(self):
        date_range = pd.date_range("2009-01", "2023-03", freq="MS")
        urls = [
            "https://{domain}/{endpoint}/yellow_tripdata_{month}.parquet".format(
                domain="d37ci6vzurychx.cloudfront.net",
                endpoint="trip-data",
                month=date.strftime("%Y-%m"),
            )
            for date in date_range
        ]
        years = [date.year for date in date_range]
        return urls, years
