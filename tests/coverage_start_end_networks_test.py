import pytest
from datetime import date, datetime
from analyzers.coverage_start_end_networks import AnalyzeCoverageStartEndNetworks
import os
from tests.checker import same_list_tuple

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_start_end_networks"


def test_get_networks_get_tuples_without_journey():
    data = {"a": 1, "b": 2}

    assert list(AnalyzeCoverageStartEndNetworks.get_networks(data)) == []
    assert list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)) == []


def test_get_networks_get_tuples_with_empty_journeys():
    data = {"a": 1, "journeys": []}

    assert list(AnalyzeCoverageStartEndNetworks.get_networks(data)) == []
    assert list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)) == []


def test_get_networks_get_tuples_without_sections():
    data = {"a": 1, "journeys": [{"user_name": "bob", "token": "1234"}]}

    assert list(AnalyzeCoverageStartEndNetworks.get_networks(data)) == []
    assert list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)) == []


def test_get_networks_get_tuples_without_coverage():
    data = {"a": 1, "journeys": [{"user_name": "bob", "token": "1234"}]}

    assert list(AnalyzeCoverageStartEndNetworks.get_networks(data)) == []
    assert list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)) == []


def test_get_networks_get_tuples_start_network_id_empty():
    data = {
        "a": 1,
        "coverages": [{"region_id": "test"}],
        "user_name": "canaltp",
        "request_date": 1487289600,
        "journeys": [
            {
                "sections": [
                    {"type": "public_transport", "network_id": ""},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": "123", "network_name": "AA"}
                ]
            }
        ]
    }
    expected_results = [{'end_network_name': 'AA', 'start_network_id': '123',
                         'start_network_name': 'AA', 'end_network_id': '123'}]

    # get_network
    result = list(AnalyzeCoverageStartEndNetworks.get_networks(data))

    assert len(result) == len(expected_results)
    assert len(result) == 1
    assert len(set(result[0].keys()) - set(expected_results[0].keys())) == 0
    assert len(set(result[0].values()) - set(expected_results[0].values())) == 0

    #get_tuples_from_stat_dict
    assert same_list_tuple(list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)),
                           [(('test', '123', 'AA', '123', 'AA', 1, date(2017, 2, 17)), 1)])


def test_get_networks_get_tuples_end_network_id_empty():
    data = {
        "a": 1,
        "coverages": [{"region_id": "test"}],
        "user_name": "canaltp",
        "request_date": 1487289600,
        "journeys": [
            {
                "sections": [
                    {"type": "public_transport", "network_id": "123", "network_name": "AA"},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": "", "network_name": "BB"}
                ]
            }
        ]
    }

    expected_results = [{'end_network_name': 'AA', 'start_network_id': '123',
                         'start_network_name': 'AA', 'end_network_id': '123'}]

    # get_network
    result = list(AnalyzeCoverageStartEndNetworks.get_networks(data))

    assert len(result) == len(expected_results)
    assert len(result) == 1
    assert len(set(result[0].keys()) - set(expected_results[0].keys())) == 0
    assert len(set(result[0].values()) - set(expected_results[0].values())) == 0

    #get_tuples_from_stat_dict
    assert same_list_tuple(list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)),
                           [(('test', '123', 'AA', '123', 'AA', 1, date(2017, 2, 17)), 1)])


def test_get_networks_get_tuples_start_end_network_id_empty():
    data = {
        "a": 1,
        "coverages": [{"region_id": "test"}],
        "user_name": "canaltp",
        "request_date": 1487289600,
        "journeys": [
            {
                "sections": [
                    {"type": "public_transport", "network_id": ""},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": ""}
                ]
            }
        ]
    }

    assert list(AnalyzeCoverageStartEndNetworks.get_networks(data)) == []
    assert list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)) == []


def test_get_networks_get_tuples_valid():
    data = {
        "a": 1,
        "coverages": [{"region_id": "test"}],
        "user_name": "canaltp",
        "request_date": 1487289600,
        "journeys": [
            {
                "sections": [
                    {"type": "public_transport", "network_id": "123", "network_name": "AA"},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": "58", "network_name": "BB"}
                ]
            }
        ]
    }
    # get_network
    result = list(AnalyzeCoverageStartEndNetworks.get_networks(data))
    expected_results = [{'end_network_name': 'AA', 'start_network_id': '123',
                         'start_network_name': 'BB', 'end_network_id': '58'}]
    assert len(result) == len(expected_results)
    assert len(result) == 1
    assert len(set(result[0].keys()) - set(expected_results[0].keys())) == 0
    assert len(set(result[0].values()) - set(expected_results[0].values())) == 0

    #get_tuples_from_stat_dict
    assert same_list_tuple(list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)),
                           [(('test', '123', 'AA', '58', 'BB', 1, date(2017, 2, 17)), 1)])



def test_get_networks_get_tuples_many_sections():
    """
        First network AA
        Last network RR
    """
    data = {
        "a": 1,
        "coverages": [{"region_id": "test"}],
        "user_name": "canaltp",
        "request_date": 1487289600,
        "journeys": [
            {
                "sections": [
                    {"type": "public_transport", "network_id": "123", "network_name": "AA"},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": "58", "network_name": "BB"},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": "12", "network_name": "RR"},
                ]
            }
        ]
    }
    # get_network
    result = list(AnalyzeCoverageStartEndNetworks.get_networks(data))
    expected_results = [{'end_network_name': 'AA', 'start_network_id': '123',
                         'start_network_name': 'RR', 'end_network_id': '12'}]
    assert len(result) == len(expected_results)
    assert len(result) == 1
    assert len(set(result[0].keys()) - set(expected_results[0].keys())) == 0
    assert len(set(result[0].values()) - set(expected_results[0].values())) == 0

    #get_tuples_from_stat_dict
    assert same_list_tuple(list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)),
                           [(('test', '123', 'AA', '12', 'RR', 1, date(2017, 2, 17)), 1)])


def test_get_networks_many_get_tuples_journeys_many_sections():
    """
        First network AA
        Last network RR
    """
    data = {
        "a": 1,
        "coverages": [{"region_id": "test"}],
        "user_name": "canaltp",
        "request_date": 1487289600,
        "journeys": [
            {
                "sections": [
                    {"type": "public_transport", "network_id": "123", "network_name": "AA"},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": "58", "network_name": "BB"},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": "12", "network_name": "RR"},
                ]
            },
            {
                "sections": [
                    {"type": "public_transport", "network_id": "123", "network_name": "AA"},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": "58", "network_name": "BB"},
                    {"type": "transfers", "network_id": ""},
                    {"type": "public_transport", "network_id": "12", "network_name": "RR"},
                ]
            }
        ]
    }
    # get_network
    result = list(AnalyzeCoverageStartEndNetworks.get_networks(data))
    expected_results = [
        {'start_network_id': '123', 'end_network_name': 'RR', 'start_network_name': 'AA', 'end_network_id': '12'},
        {'start_network_id': '123', 'end_network_name': 'RR', 'start_network_name': 'AA', 'end_network_id': '12'}
    ]

    assert len(result) == len(expected_results)
    assert len(result) == 2
    assert len(set(result[0].keys()) - set(expected_results[0].keys())) == 0
    assert len(set(result[0].values()) - set(expected_results[0].values())) == 0
    assert len(set(result[1].keys()) - set(expected_results[1].keys())) == 0
    assert len(set(result[1].values()) - set(expected_results[1].values())) == 0

    #get_tuples_from_stat_dict
    assert same_list_tuple(list(AnalyzeCoverageStartEndNetworks.get_tuples_from_stat_dict(data)),
                           [(('test', '123', 'AA', '12', 'RR', 1, date(2017, 2, 17)), 1),
                            (('test', '123', 'AA', '12', 'RR', 1, date(2017, 2, 17)), 1)])


def test_coverage_start_end_network(spark):
    start_date = date(2017, 2, 17)
    end_date = date(2017, 2, 17)
    analyzer = AnalyzeCoverageStartEndNetworks(storage_path=path, start_date=start_date, end_date=end_date,
                                               spark_session=spark, database=None,
                                               current_datetime=datetime(2017, 2, 15, 15, 10))

    same_list_tuple(list(analyzer.get_data(rdd_mode=True)),
                    [('fr-cha', 'network:SNC', 'SNCF', 'network:SNC', 'SNCF', 0, date(2017, 2, 17), 1),
                     ('fr-cha', 'network:SNC', 'SNCF', 'network:SNC', 'SNCF', 1, date(2017, 2, 17), 1)])

    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [AnalyzeCoverageStartEndNetworks] [120]"