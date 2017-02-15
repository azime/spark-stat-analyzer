import pytest
from includes.database import Database


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
