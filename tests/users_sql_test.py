import pytest
from includes import common
import users_sql

pytestmark = pytest.mark.usefixtures("spark")


def test_users_sql(spark):
    file_list = 'tests/fixtures/users.json.log'
    df = common.get_sql_data_frame(spark, file_list)
    results = users_sql.process(df)
    expected_results = {666: {'last_user_name': u'Billy', 'first_date': 1484467930},
                        42: {'last_user_name': u'Kenny last name', 'first_date': 1484459770},
                        15: {'last_user_name': u'Bobby new name', 'first_date': 1484467930}}
    assert len(results) == len(expected_results)
    formatted_results = {elt.user_id: {"last_user_name": elt.last_user_name, "first_date": elt.first_date}
                         for elt in results}
    
    for row_id, row in formatted_results.items():
        assert row == expected_results[row_id]
