import pytest
from datetime import date, datetime
from analyzers.coverage_journeys import AnalyzeCoverageJourneys
import os

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_journeys"

def test_count_journeys(spark):
    start_date = date(2017, 1, 20)
    end_date = date(2017, 1, 20)
    expected_results = [
        (datetime.utcfromtimestamp(1484993662).date(), u'fr-foo', 0, 9),
        (datetime.utcfromtimestamp(1484993662).date(), u'fr-bar', 0, 8)
    ]

    analyzer = AnalyzeCoverageJourneys(storage_path=path,
                                       start_date=start_date,
                                       end_date=end_date,
                                       spark_session=spark,
                                       database=None,
                                       current_datetime=datetime(2017, 2, 15, 15, 10))
    results = analyzer.get_data(rdd_mode=True)

    assert len(results) == len(expected_results)
    for result in results:
        assert result in expected_results

    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [CoverageJourneys] [120]"
