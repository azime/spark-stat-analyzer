import pytest
from datetime import date, datetime
from analyzers.coverage_journeys_requests_params import AnalyzeCoverageJourneysRequestsParams
import os

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_journeys_request_params"

def test_no_journeys(spark):
    start_date = date(2017, 1, 20)
    end_date = date(2017, 1, 20)

    analyzer = AnalyzeCoverageJourneysRequestsParams(storage_path=path,
                                         start_date=start_date,
                                         end_date=end_date,
                                         spark_session=spark,
                                         database=None,
                                         current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data()
    assert len(results) == 0

def test_no_wheelchair_journeys_request(spark):
    start_date = date(2017, 1, 21)
    end_date = date(2017, 1, 21)

    analyzer = AnalyzeCoverageJourneysRequestsParams(storage_path=path,
                                         start_date=start_date,
                                         end_date=end_date,
                                         spark_session=spark,
                                         database=None,
                                         current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data()
    assert len(results) == 0

def test_count_wheelchair_journeys(spark):
    start_date = date(2017, 1, 22)
    end_date = date(2017, 1, 22)
    expected_results = [
        (datetime.utcfromtimestamp(1484993662).date(),u'fr-foo',0,2),
        (datetime.utcfromtimestamp(1484993662).date(),u'fr-bar',0,1)
    ]

    analyzer = AnalyzeCoverageJourneysRequestsParams(storage_path=path,
                                            start_date=start_date,
                                            end_date=end_date,
                                            spark_session=spark,
                                            database=None,
                                            current_datetime=datetime(2017, 2, 15, 15, 10))
    results = analyzer.get_data()

    assert len(results) == len(expected_results)
    for result in results:
        assert result in expected_results
