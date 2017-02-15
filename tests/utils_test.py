import pytest
from datetime import date
import os
from includes.utils import check_and_get_path, date_format, analyzer_value, sub_iterable
from analyzers.token_stat import AnalyzeToken
from analyzers.users_sql import AnalyseUsersSql
from analyzers.requests_calls import AnalyzeRequest
from tests.checker import same_list_tuple


def test_path_invalid():
    with pytest.raises(NotImplementedError):
        check_and_get_path("aaa")


def test_path_valid():
    assert os.getcwd() == check_and_get_path(os.getcwd())


def test_date_invalid():
    with pytest.raises(ValueError):
        date_format("20170101")
    with pytest.raises(ValueError):
        date_format("201701")


def test_date_valid():
    assert date(2017, 1, 1) == date_format("2017-01-01")


def test_option_invalid():
    with pytest.raises(ValueError):
        analyzer_value("bob")


def test_analyzer_valid():
    values = {"token_stat": AnalyzeToken, "users_sql": AnalyseUsersSql, "requests_calls": AnalyzeRequest}
    for value in values:
        assert values[value] == analyzer_value(value)


def test_analyzer_upper_lower():
    values = {"TOKEN_stat": AnalyzeToken, "users_SQL": AnalyseUsersSql, "requests_CALLS": AnalyzeRequest}
    for value in values:
        assert values[value] == analyzer_value(value)


def test_sub_iterable_empty():
    values = sub_iterable([], 2)
    assert len([v for v in values]) == 0


def test_sub_iterable():
    result = sub_iterable([1, 2, 3, 4, 5], 2)
    expected_results = [(1, 2), (3, 4), (5,)]

    assert same_list_tuple([v for v in result], expected_results)
