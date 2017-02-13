import pytest
from datetime import date
from analyzers.requests_calls import AnalyzeRequest
import os

pytestmark = pytest.mark.usefixtures("spark")


def test_requests_calls(spark):
    path = os.getcwd() + "/tests/fixtures/requests_calls"
    start_date = date(2017, 1, 1)
    end_date = date(2017, 1, 1)

    tokenstat = AnalyzeRequest(storage_path=path,
                               start_date=start_date,
                               end_date=end_date,
                               spark_context=spark,
                               database=None)

    files = tokenstat.get_files_to_analyze()

    expected_files = [path + '/2017/01/01/requests_calls.json.log']


    assert len(files) == len(expected_files)
    assert len(set(files) - set(expected_files)) == 0
