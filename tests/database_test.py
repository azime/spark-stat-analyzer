import pytest
from includes.database import Database


def fixture_database():
    return Database(dbname="dbname", user="user", password="password", schema="sh", auto_connect=False)


def test_params_database():
    d = fixture_database()
    assert d.connection_string == "host='localhost' port='5432' dbname='dbname' user='user' password='password'"
    assert d.schema == "sh"
