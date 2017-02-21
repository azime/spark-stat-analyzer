import pytest
from analyzers import AnalyzeCoverageModes


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

