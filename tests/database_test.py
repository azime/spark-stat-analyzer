import pytest
from includes.database import Database
from datetime import date


def fixture_database():
    return Database(dbname="dbname", user="user", password="password", schema="sh", auto_connect=False)


def test_params_database():
    d = fixture_database()
    assert d.connection_string == "host='localhost' port='5432' dbname='dbname' user='user' password='password'"
    assert d.schema == "sh"


def test_format_insert_query():
    columns = ["c1", "c2"]
    data = [("a", "b"), ("c", "d")]
    d = fixture_database()
    assert d.format_insert_query("table1", columns, data) == "INSERT INTO sh.table1 (c1, c2) VALUES %s,%s"


def test_format_delete_query():
    d = fixture_database()
    assert d.format_delete_query("table1", date(2017, 1, 15), date(2017, 1, 16)) == \
           "DELETE FROM sh.table1 WHERE request_date >= ('2017-01-15' :: date) " \
           "AND request_date < ('2017-01-16' :: date) + interval '1 day'"
