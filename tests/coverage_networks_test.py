import pytest
from datetime import date, datetime
from analyzers.coverage_networks import AnalyzeCoverageNetworks
import os

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_networks"
date_2017_02_15 = datetime.utcfromtimestamp(1484473662).date()
date_2017_02_18 = datetime.utcfromtimestamp(1484758062).date()

def test_coverage_networks_no_journeys(spark):
    start_date = date(2017, 1, 22)
    end_date = date(2017, 1, 22)

    analyzer = AnalyzeCoverageNetworks(storage_path=path,
                                       start_date=start_date,
                                       end_date=end_date,
                                       spark_session=spark,
                                       database=None,
                                       current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data(rdd_mode=True)
    assert len(results) == 0

def test_coverage_networks_no_valid_found(spark):
    start_date = date(2017, 1, 20)
    end_date = date(2017, 1, 20)

    analyzer = AnalyzeCoverageNetworks(storage_path=path,
                                       start_date=start_date,
                                       end_date=end_date,
                                       spark_session=spark,
                                       database=None,
                                       current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data(rdd_mode=True)
    assert len(results) == 0

    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [CoverageNetworks] [120]"

@pytest.mark.parametrize("day,expected_results", [
    (15,
     [(u'auv', u'network:CD64', u'one network with different name', 1, date_2017_02_15, 1),
      (u'auv', u'network:CD21', u'another network 21', 1, date_2017_02_15, 1),
      (u'auv', u'network:CD63', u'one network 63', 1, date_2017_02_15, 2),
      (u'auv', u'network:CD64', u'one network 64', 1, date_2017_02_15, 1),
      (u'auv', u'network:CD65', u'one network 65', 1, date_2017_02_15, 1)]
     ),
    (18,
     [(u'auv', u'network:CD15', u'one network 15', 1, date_2017_02_18, 1),
      (u'transilien', u'network:CD14', u'one network 14', 0, date_2017_02_18, 3),
      (u'auv', u'network:CD14', u'one network with different name', 1, date_2017_02_18, 1),
      (u'transilien', u'network:CD13', u'one network 13', 0, date_2017_02_18, 1),
      (u'transilien', u'network:WTF007', u'another network wtf', 0, date_2017_02_18, 2),
      (u'auv', u'network:CD21', u'another network 21', 1, date_2017_02_18, 1),
      (u'transilien', u'network:CD13', u'one network new name 13', 0, date_2017_02_18, 2),
      (u'auv', u'network:CD13', u'one network 13', 1, date_2017_02_18, 2),
      (u'auv', u'network:CD14', u'one network 14', 1, date_2017_02_18, 1)]),
])
def test_coverage_networks_count(spark, day, expected_results):
    start_date = date(2017, 1, day)
    end_date = date(2017, 1, day)

    analyzer = AnalyzeCoverageNetworks(storage_path=path,
                                       start_date=start_date,
                                       end_date=end_date,
                                       spark_session=spark,
                                       database=None,
                                       current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data(rdd_mode=True)
    assert len(results) == len(expected_results)

    for result in results:
        assert result in expected_results
