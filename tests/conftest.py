import pytest
import findspark
findspark.init()
from pyspark.sql import SparkSession
import logging

@pytest.fixture(scope="session")
def spark(request):
    spark = SparkSession.builder \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    request.addfinalizer(lambda: spark.sparkContext.stop())

    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)
    return spark