import pytest
from datetime import date
from includes import common
import token_stat

pytestmark = pytest.mark.usefixtures("spark")


def test_token_stat(spark):
    file_list = 'tests/fixtures/token_stat.log'
    df = common.get_sql_data_frame(spark, file_list)
    results = token_stat.process(df)
    expected_results = [(u'token:3', date(2017, 1, 15), 6),
                        (u'token:1', date(2017, 1, 15), 2),
                        (u'token:2', date(2017, 1, 16), 1),
                        (u'token:2', date(2017, 1, 15), 1),
                        (u'token:1', date(2017, 1, 16), 2)]
    assert len(results) == len(expected_results)
    assert len(set(results) - set(expected_results)) == 0
