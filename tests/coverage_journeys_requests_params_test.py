import pytest
from datetime import date, datetime
from analyzers import AnalyzeCoverageJourneysRequestsParams
import os

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_journeys_request_params"


def test_no_journeys(spark):
    start_date = date(2017, 1, 20)
    end_date = date(2017, 1, 20)

    analyzer = AnalyzeCoverageJourneysRequestsParams(storage_path=path, start_date=start_date, end_date=end_date,
                                                     spark_session=spark, database=None,
                                                     current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data(rdd_mode=True)
    assert len(results) == 0
    files = analyzer.get_files_to_analyze()
    assert files == [os.path.join(path, "2017/01/20/no_journeys.json.log")]


def test_no_wheelchair_journeys_request(spark):
    start_date = date(2017, 1, 21)
    end_date = date(2017, 1, 21)

    analyzer = AnalyzeCoverageJourneysRequestsParams(storage_path=path, start_date=start_date, end_date=end_date,
                                                     spark_session=spark, database=None,
                                                     current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data(rdd_mode=True)
    assert len(results) == 0
    files = analyzer.get_files_to_analyze()
    assert files == [os.path.join(path, "2017/01/21/no_wheelchair_journeys.json.log")]


def test_count_wheelchair_journeys(spark):
    start_date = date(2017, 1, 22)
    end_date = date(2017, 1, 22)
    expected_results = [
        (datetime.utcfromtimestamp(1484993662).date(), u'fr-foo', 0, 2),
        (datetime.utcfromtimestamp(1484993662).date(), u'fr-bar', 0, 1)
    ]

    analyzer = AnalyzeCoverageJourneysRequestsParams(storage_path=path, start_date=start_date, end_date=end_date,
                                                     spark_session=spark, database=None,
                                                     current_datetime=datetime(2017, 2, 15, 15, 10))
    results = analyzer.get_data(rdd_mode=True)

    assert len(results) == len(expected_results)
    for result in results:
        assert result in expected_results

    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [CoverageJourneysRequestsParams] [120]"
