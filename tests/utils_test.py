import pytest
from datetime import date
import os
from includes.utils import check_and_get_path, date_format, analyzer_value, sub_iterable
from analyzers import AnalyzeTokens, AnalyseUsersSql, AnalyzeRequest, \
    AnalyzeCoverageModes, AnalyzeErrors, AnalyzeCoverageStopAreas, AnalyzeCoverageJourneysTransfers, \
    AnalyzeCoverageJourneysRequestsParams, AnalyzeCoverageJourneys, AnalyzeCoverageNetworks, \
    AnalyzeCoverageStartAndNetworks
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
    values = {"token_stats": AnalyzeTokens, "users": AnalyseUsersSql, "requests_calls": AnalyzeRequest,
              "error_stats": AnalyzeErrors, "coverage_stop_areas": AnalyzeCoverageStopAreas,
              "coverage_modes": AnalyzeCoverageModes,
              "coverage_journeys_transfers": AnalyzeCoverageJourneysTransfers,
              "coverage_journeys_requests_params": AnalyzeCoverageJourneysRequestsParams,
              "coverage_journeys": AnalyzeCoverageJourneys,
              "coverage_networks": AnalyzeCoverageNetworks,
              "coverage_start_end_networks": AnalyzeCoverageStartAndNetworks}
    for value in values:
        assert values[value] == analyzer_value(value)


def test_analyzer_upper_lower():
    values = {"TOKEN_stats": AnalyzeTokens, "USERS": AnalyseUsersSql, "requests_CALLS": AnalyzeRequest,
              "ERROR_stats": AnalyzeErrors, "coverage_STOP_areas": AnalyzeCoverageStopAreas,
              "coverage_MODES": AnalyzeCoverageModes,
              "coverage_JOURNEYS_transfers": AnalyzeCoverageJourneysTransfers,
              "coverage_journeys_requests_PARAMS": AnalyzeCoverageJourneysRequestsParams,
              "covERAGE_journeys": AnalyzeCoverageJourneys,
              "coverage_netWORKS": AnalyzeCoverageNetworks,
              "coverage_start_END_networks": AnalyzeCoverageStartAndNetworks}
    for value in values:
        assert values[value] == analyzer_value(value)


def test_sub_iterable_empty():
    values = sub_iterable([], 2)
    assert len([v for v in values]) == 0


def test_sub_iterable_format_tuple():
    result = sub_iterable([1, 2, 3, 4, 5], 2)
    expected_results = [(1, 2), (3, 4), (5,)]

    assert same_list_tuple([v for v in result], expected_results)


def test_sub_iterable_format_list():
    result = [v for v in sub_iterable([1, 2, 3, 4, 5], 2, list)]

    expected_results = [[1, 2], [3, 4], [5]]
    assert result[0] == expected_results[0]
    assert result[1] == expected_results[1]
    assert result[2] == expected_results[2]
    assert len(result) == len(expected_results)


def test_sub_iterable_format_string():
    result = [v for v in sub_iterable("iterable", 2, list)]

    expected_results = [['i', 't'], ['e', 'r'], ['a', 'b'], ['l', 'e']]
    assert result[0] == expected_results[0]
    assert result[1] == expected_results[1]
    assert result[2] == expected_results[2]
    assert result[3] == expected_results[3]
    assert len(result) == len(expected_results)
