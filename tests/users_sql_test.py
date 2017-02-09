import pytest
from includes import common
import users_sql
from pyspark.sql import Row

pytestmark = pytest.mark.usefixtures("spark")

def test_users_sql(spark):
    file_list = 'tests/fixtures/users.json'
    df = common.get_sql_data_frame(spark, file_list)
    results = users_sql.process(df)
    expected_results = [Row(user_id=15, last_user_name=u'Bobby new name', first_date=1484467930), Row(user_id=42, last_user_name=u'Kenny last name', first_date=1484459770), Row(user_id=666, last_user_name=u'Billy', first_date=1484467930)]
    # Row created element lose order so we can't compare strictly with assert ==
    assert len(results) == len(expected_results)
    for row in results:
        for expected_row in expected_results:
            if row.user_id == expected_row.user_id:
                assert row.last_user_name == expected_row.last_user_name
                assert row.first_date == expected_row.first_date