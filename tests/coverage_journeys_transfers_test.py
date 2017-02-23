import pytest
import os
from datetime import date, datetime
from analyzers import AnalyzeCoverageJourneysTransfers
from checker import same_list_tuple

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_journeys_transfers"


def test_journeys_transfers_get_tuples_from_stat_dict_without_journey():
    data = {"a": 1, "b": 2}

    result = AnalyzeCoverageJourneysTransfers.get_tuples_from_stat_dict(data)
    assert result == []


def test_journeys_transfers_get_tuples_from_stat_dict_with_empty_journeys():
    data = {"a": 1, "journeys": []}
    result = AnalyzeCoverageJourneysTransfers.get_tuples_from_stat_dict(data)
    assert result == []


def test_journeys_transfers_get_tuples_from_stat_dict_without_sections():
    data = {"a": 1, "journeys": [{"user_name": "bob", "token": "1234"}]}
    result = AnalyzeCoverageJourneysTransfers.get_tuples_from_stat_dict(data)
    assert result == []


def test_journeys_transfers_get_tuples_from_stat_dict_without_coverage():
    data = {"a": 1, "journeys": [{"user_name": "bob", "token": "1234"}]}
    result = AnalyzeCoverageJourneysTransfers.get_tuples_from_stat_dict(data)
    assert result == []


def test_journeys_transfers_coverage_modes_count(spark):
    expected_results = [
        ('auv', 0, 1, date(2017, 1, 15), 1),
        ('auv', 2, 1, date(2017, 1, 15), 1)
    ]
    analyzer = AnalyzeCoverageJourneysTransfers(storage_path=path, start_date=date(2017, 1, 15),
                                                end_date=date(2017, 1, 15), spark_session=spark, database=None)

    results = analyzer.get_data()
    print(results)
    assert same_list_tuple(results, expected_results)


def test_journeys_transfers_coverage_modes_without_journey(spark):
    start_date = date(2017, 1, 22)
    end_date = date(2017, 1, 22)
    analyzer = AnalyzeCoverageJourneysTransfers(storage_path=path, start_date=start_date, end_date=end_date,
                                                spark_session=spark, database=None,
                                                current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data()
    assert len(results) == 0
    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[spark-stat-analyzer] [OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [CoverageJourneysTransfers] [120]"
