import pytest
from datetime import date
from includes import common
import token_stat

pytestmark = pytest.mark.usefixtures("spark")

def test_token_stat(spark):
    file_list = 'tests/fixtures/token_stat.json'
    df = common.get_sql_data_frame(spark, file_list)
    results = token_stat.process(df)
    expected_results = [(u'da821360-5fdc-47cb-acc3-9025e56f0003', date(2017, 1, 15), 6),
                        (u'da821360-5fdc-47cb-acc3-9025e56f0001', date(2017, 1, 15), 2),
                        (u'da821360-5fdc-47cb-acc3-9025e56f0002', date(2017, 1, 16), 1),
                        (u'da821360-5fdc-47cb-acc3-9025e56f0002', date(2017, 1, 15), 1),
                        (u'da821360-5fdc-47cb-acc3-9025e56f0001', date(2017, 1, 16), 2)]
    assert results == expected_results
