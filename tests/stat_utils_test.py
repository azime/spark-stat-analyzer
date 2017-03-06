from analyzers.stat_utils import region_id, is_internal_call, request_date
from datetime import date


def test_region_id():
    dict = {'coverages': [{'region_id': 'fr-foo'}]}
    assert region_id(dict) == 'fr-foo'


def test_is_internal_call():
    dict = {'user_name': 'foo-canaltp-bar'}
    assert is_internal_call(dict) == 1
    dict = {'user_name': 'foo-bar'}
    assert is_internal_call(dict) == 0


def test_request_date():
    dict = {'request_date': 1484993662}
    assert request_date(dict) == date(2017, 1, 21)
