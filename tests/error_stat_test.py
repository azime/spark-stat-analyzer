import pytest
from datetime import date, datetime
from analyzers import AnalyzeError
import os

pytestmark = pytest.mark.usefixtures("spark")

def test_error_stat(spark):
    path = os.getcwd() + "/tests/fixtures/error_stat"
    start_date = date(2017, 1, 15)
    end_date = date(2017, 1, 15)

    analyzer = AnalyzeErrors(storage_path=path,
                             start_date=start_date,
                             end_date=end_date,
                             spark_session=spark,
                             database=None,
                             current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data()
    # each record have a field that change to test the groupBy clause (ie: we should only group if
    # region_id, api, request_date,  user_id, application_name, err_id and is_internal_call are equals)
    expected_results = [ \
            {"region_id": u"fr-foo", "api": u"v1.some_api", "request_date": u"2017-01-17", "user_id": 42, \
            "application_name": u"my_app", "err_id": u"some_error_id", "is_internal_call": 0, "count": 1},
            {"region_id": u"fr-idf", "api": u"v1.another_api", "request_date": u"2017-01-17", "user_id": 42, \
            "application_name": u"my_app", "err_id": u"some_error_id", "is_internal_call": 0, "count": 1},
            {"region_id": u"fr-idf", "api": u"v1.some_api", "request_date": u"2013-11-16", "user_id": 42, \
            "application_name": u"my_app", "err_id": u"some_error_id", "is_internal_call": 0, "count": 1},
            {"region_id": u"fr-idf", "api": u"v1.some_api", "request_date": u"2017-01-17", "user_id": 43, \
            "application_name": u"my_app", "err_id": u"some_error_id", "is_internal_call": 0, "count": 1},
            {"region_id": u"fr-idf", "api": u"v1.some_api", "request_date": u"2017-01-17", "user_id": 42, \
            "application_name": u"another_app", "err_id": u"some_error_id", "is_internal_call": 0, "count": 1},
            {"region_id": u"fr-idf", "api": u"v1.some_api", "request_date": u"2017-01-17", "user_id": 42, \
            "application_name": u"my_app", "err_id": u"another_error_id", "is_internal_call": 0, "count": 1},
            {"region_id": u"fr-idf", "api": u"v1.some_api", "request_date": u"2017-01-17", "user_id": 42, \
            "application_name": u"my_app", "err_id": u"some_error_id", "is_internal_call": 1, "count": 1},
            {"region_id": u"fr-idf", "api": u"v1.some_api", "request_date": u"2017-01-17", "user_id": 42, \
            "application_name": u"my_app", "err_id": u"some_error_id", "is_internal_call": 0, "count": 3},
    ]
    results_to_compare = []
    for result in results:
        results_to_compare.append(result.asDict())

    assert len(expected_results) == len(results_to_compare)

    for expected_result in expected_results:
        assert expected_result in results_to_compare

    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[spark-stat-analyzer] [OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [ErrorStatsUpdater] [120]"
