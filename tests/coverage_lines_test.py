import pytest
from datetime import date, datetime
from analyzers import AnalyzeCoverageLines
import os

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_lines"


def test_coverage_no_lines(spark):
    start_date = date(2017, 2, 20)
    end_date = date(2017, 2, 20)

    analyzer = AnalyzeCoverageLines(storage_path=path, start_date=start_date, end_date=end_date,
                                        spark_session=spark, database=None,
                                        current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data(rdd_mode=True)
    assert len(results) == 0


def test_coverage_lines(spark):
    start_date = date(2017, 2, 21)
    end_date = date(2017, 2, 21)

    analyzer = AnalyzeCoverageLines(storage_path=path, start_date=start_date, end_date=end_date,
                                        spark_session=spark, database=None,
                                        current_datetime=datetime(2017, 2, 15, 15, 10))

    results = analyzer.get_data(rdd_mode=True)
    assert len(results) == 7

    expected_results = [
        ("fr-cen", "public_transport", "line:id:foo", "line_code", "network:foo", "network-name", 0, date(2017, 2, 21), 1),
        ("fr-cen", "some_type", "line:id:foo", "line_code", "network:foo", "network-name", 0, date(2017, 2, 21), 1),
        ("fr-cen", "public_transport", "line:id:bar", "line_code", "network:foo", "network-name", 0, date(2017, 2, 21), 1),
        ("fr-cen", "public_transport", "line:id:foo", "line_code_1", "network:foo", "network-name", 0, date(2017, 2, 21), 1),
        ("fr-cen", "public_transport", "line:id:foo", "line_code", "network:bar", "network-name", 0, date(2017, 2, 21), 1),
        ("fr-cen", "public_transport", "line:id:foo", "line_code", "network:foo", "network-name-1", 0, date(2017, 2, 21), 1),
        ("fr-bar", "public_transport", "line:id:foo", "line_code", "network:foo", "network-name", 0, date(2017, 2, 21), 2)
    ]

    for result in results:
        assert result in expected_results
