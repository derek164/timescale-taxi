from pathlib import Path

import backoff
import psycopg2
import yaml
from jsonschema import validate
from psycopg2 import OperationalError


class TimeScaleClient:
    def __init__(self, database):
        self.database = database
        self.properties = self._set_properties()

    @property
    @backoff.on_exception(backoff.expo, OperationalError, max_tries=5)
    def connection(self):
        try:
            return psycopg2.connect(**self.properties)
        except OperationalError as error:
            raise error

    @property
    def cursor(self):
        return self.connection.cursor()

    def _set_properties(self):
        properties = self._get_properties()
        self._validate_properties(properties)
        return properties

    def _get_properties(self):
        config = "{pwd}/databases/{database}.yaml".format(
            pwd=Path(__file__).parent, database=self.database
        )
        with open(config, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as e:
                raise e

    def _validate_properties(self, properties):
        validate(
            instance=properties,
            schema={
                "type": "object",
                "properties": {
                    "dbname": {"type": "string"},
                    "user": {"type": "string"},
                    "password": {"type": "string"},
                    "host": {"type": "string"},
                    "port": {"type": "number"},
                    "sslmode": {"type": "string"},
                },
                "required": ["dbname", "user", "password", "host", "port"],
            },
        )
