import pytest
from datetime import date, datetime
from analyzers import AnalyzeCoverageStopArea
import os

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_stop_areas"
date_2017_02_15 = datetime.utcfromtimestamp(1484473662).date()
date_2017_02_18 = datetime.utcfromtimestamp(1484758062).date()

def test_coverage_stop_area_no_journeys(spark):
    start_date = date(2017, 1, 22)
    end_date = date(2017, 1, 22)

    analyzer = AnalyzeCoverageStopAreas(storage_path=path,
                                       start_date=start_date,
                                       end_date=end_date,
                                       spark_session=spark,
                                       database=None,
                                       current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data()
    assert len(results) == 0

def test_coverage_stop_area_no_valid_found(spark):
    start_date = date(2017, 1, 20)
    end_date = date(2017, 1, 20)

    analyzer = AnalyzeCoverageStopAreas(storage_path=path,
                                       start_date=start_date,
                                       end_date=end_date,
                                       spark_session=spark,
                                       database=None,
                                       current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data()
    assert len(results) == 0

    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[spark-stat-analyzer] [OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [CoverageStopAreas] [120]"

@pytest.mark.parametrize("day,expected_results", [
    (15,
     [(u'auv', u'sa_2', u'stop 2', '', '', '', '', 1, date_2017_02_15, 2),
      (u'auv', u'sa_4', u'stop 4', '', '', '', '', 1, date_2017_02_15, 2),
      (u'auv', u'sa_1', u'stop 1', '', '', '', '', 1, date_2017_02_15, 1),
      (u'auv', u'sa_3', u'stop 3', '', '', '', '', 1, date_2017_02_15, 2)]),
    (18,
     [(u'auv', u'sa_4', u'stop 4', '', '', '', '', 1, date_2017_02_18, 2),
      (u'npdc', u'sa_4', u'stop 4', u'admin:xxx4', '', '', '', 0, date_2017_02_18, 1),
      (u'auv', u'sa_3', u'stop 3', '', '', '', '', 1, date_2017_02_18, 2),
      (u'auv', u'sa_2', u'stop 2', '', '', '', '', 1, date_2017_02_18, 2),
      (u'auv', u'sa_1', u'stop 1', '', '', '', '', 1, date_2017_02_18, 1),
      (u'npdc', u'sa_3', u'stop 3', '', u'on the styx', '', '', 0, date_2017_02_18, 1),
      (u'npdc', u'sa_1', u'stop 1', '', '', u'123456', u'12', 0, date_2017_02_18, 1),
      (u'npdc', u'sa_3', u'stop 3', '', '', '', '', 0, date_2017_02_18, 1),
      (u'npdc', u'sa_2', u'stop 2', '', '', u'987654', u'98', 0, date_2017_02_18, 2)]),
])
def test_coverage_stop_area_count(spark, day, expected_results):
    start_date = date(2017, 1, day)
    end_date = date(2017, 1, day)

    analyzer = AnalyzeCoverageStopAreas(storage_path=path,
                                       start_date=start_date,
                                       end_date=end_date,
                                       spark_session=spark,
                                       database=None,
                                       current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data()

    assert len(results) == len(expected_results)

    for result in results:
        assert result in expected_results
