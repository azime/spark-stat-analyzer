import pytest
from datetime import date, datetime
from analyzers.requests_calls import AnalyzeRequest
import os
from tests.checker import same_list_tuple

pytestmark = pytest.mark.usefixtures("spark")


def test_requests_calls(spark):
    path = os.getcwd() + "/tests/fixtures/requests_calls"
    start_date = date(2017, 1, 1)
    end_date = date(2017, 1, 1)

    analyze_request = AnalyzeRequest(storage_path=path,
                                     start_date=start_date,
                                     end_date=end_date,
                                     spark_context=spark,
                                     database=None,
                                     current_datetime=datetime(2017, 2, 15, 15, 10))

    files = analyze_request.get_files_to_analyze()

    expected_files = [path + '/2017/01/01/requests_calls.json.log']

    assert len(files) == len(expected_files)
    assert len(set(files) - set(expected_files)) == 0
    result = analyze_request.get_data()
    expected_results = [(u'fr-auv', u'v1.journeys', 22, u'test filbleu', 0, u'2017-01-01', 1, 1, 0, 0),
                        (u'region:2', u'v1.pt_objects', 25, u'', 0, u'2017-01-01', 1, 2, 2, 0),
                        (u'', u'v1.coverage', 51, u'', 0, u'2017-01-01', 1, 1, 1, 0),
                        (u'region:2', u'v1.networks.collection', 25, u'', 0, u'2017-01-01', 1, 1, 1, 4),
                        (u'region:1', u'v1.stop_areas.collection', 51, u'', 0, u'2017-01-01', 1, 3, 3, 75)]

    assert same_list_tuple(result, expected_results)
    assert analyze_request.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[spark-stat-analyzer] [OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [RequestCallsUpdater] [120]"


def test_requests_calls_without_journeys(spark):
    path = os.getcwd() + "/tests/fixtures/requests_calls"
    start_date = date(2017, 1, 2)
    end_date = date(2017, 1, 2)

    analyze_request = AnalyzeRequest(storage_path=path,
                                     start_date=start_date,
                                     end_date=end_date,
                                     spark_context=spark,
                                     database=None)

    files = analyze_request.get_files_to_analyze()

    expected_files = [path + '/2017/01/02/requests_calls.json.log']

    assert len(files) == len(expected_files)
    assert len(set(files) - set(expected_files)) == 0
    result = analyze_request.get_data()
    expected_results = [(u'region:2', u'v1.pt_objects', 25, u'', 0, u'2017-01-01', 1, 2, 2, 0),
                        (u'', u'v1.coverage', 51, u'', 0, u'2017-01-01', 1, 1, 1, 0),
                        (u'region:2', u'v1.networks.collection', 25, u'', 0, u'2017-01-01', 1, 1, 1, 4),
                        (u'region:1', u'v1.stop_areas.collection', 51, u'', 0, u'2017-01-01', 1, 3, 3, 75)]
    assert same_list_tuple(result, expected_results)

