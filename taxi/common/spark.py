import os
from pathlib import Path

from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import KryoSerializer, SedonaKryoRegistrator


class SparkSedonaFactory:
    @staticmethod
    def session():
        extra_jars_dir = Path(os.environ["SPARK_HOME"]) / "extra_jars"
        extra_jars = [jar.as_posix() for jar in extra_jars_dir.glob("*.jar")]
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("my_sedona_app")
            .config("spark.serializer", KryoSerializer.getName)
            .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
            .config(
                "spark.jars.packages",
                "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.3.1-incubating,"
                "org.datasyslab:geotools-wrapper:1.3.0-27.2",
            )
            .config("spark.jars", ",".join(extra_jars))
            .config("sedona.global.charset", "utf8")
            .getOrCreate()
        )
        SedonaRegistrator.registerAll(spark)
        return spark
