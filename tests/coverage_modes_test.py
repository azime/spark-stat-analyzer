import pytest
import os
from datetime import date, datetime
from analyzers import AnalyzeCoverageModes
from tests.checker import same_list_tuple

pytestmark = pytest.mark.usefixtures("spark")


def test_get_tuples_from_stat_dict_without_journey():
    data = {"a": 1, "b": 2}
    result = AnalyzeCoverageModes.get_tuples_from_stat_dict(data)
    assert result == []


def test_get_tuples_from_stat_dict_with_empty_journeys():
    data = {"a": 1, "journeys": []}
    result = AnalyzeCoverageModes.get_tuples_from_stat_dict(data)
    assert result == []


def test_get_tuples_from_stat_dict_without_sections():
    data = {"a": 1, "journeys": [{"user_name": "bob", "token": "1234"}]}
    result = AnalyzeCoverageModes.get_tuples_from_stat_dict(data)
    assert result == []


def test_get_tuples_from_stat_dict_without_coverage():
    data = {"a": 1, "journeys": [{"user_name": "bob", "token": "1234"}]}
    result = AnalyzeCoverageModes.get_tuples_from_stat_dict(data)
    assert result == []


def test_coverage_modes_count(spark):
    path = os.getcwd() + "/tests/fixtures/coverage_modes"
    expected_results = [
        ('auv', 'public_transport', 'car', 'BUS', 'BUS 1', 1, date(2017, 1, 15), 2),
        ('auv', 'public_transport', '', 'commercial_mode:RER', 'RER', 1, date(2017, 1, 15), 2)
    ]
    analyzer = AnalyzeCoverageModes(storage_path=path, start_date=date(2017, 1, 15),
                                    end_date=date(2017, 1, 15), spark_session=spark, database=None)

    results = analyzer.get_data(rdd_mode=True)
    assert same_list_tuple(results, expected_results)


def test_coverage_modes_without_journey(spark):
    path = os.getcwd() + "/tests/fixtures/coverage_modes"
    start_date = date(2017, 1, 22)
    end_date = date(2017, 1, 22)
    analyzer = AnalyzeCoverageModes(storage_path=path, start_date=start_date, end_date=end_date, spark_session=spark,
                                    database=None,
                                    current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data(rdd_mode=True)
    assert len(results) == 0
    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
       "[OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [CoverageModes] [120]"
