import pytest
import os
from includes.config import Config


def assert_config(conf):
    assert conf.host == "localhost"
    assert conf.port == 5432
    assert conf.dbname == "statistics"
    assert conf.user == "spark"
    assert conf.password == "spark"
    assert conf.schema == "stat_compiled"
    assert conf.insert_count == 100


def get_file(filename):
    return os.getcwd() + "/tests/fixtures/config/" + filename


def test_file_valid():
    settings = get_file("settings.json")
    assert_config(Config(settings))


def test_file_without_schema():
    settings = get_file("settings_without_schema.json")
    assert_config(Config(settings))


def test_file_without_port():
    settings = get_file("settings_without_port.json")
    assert_config(Config(settings))


def test_file_without_insert_count():
    settings = get_file("settings_without_insert_count.json")
    assert_config(Config(settings))


def test_file_without_database():
    settings = get_file("settings_without_database.json")
    with pytest.raises(ValueError) as excinfo:
        Config(settings)
    assert str(excinfo.value) == "Config is not valid, a database is needed"


def test_file_without_dbname():
    settings = get_file("settings_without_dbname.json")
    with pytest.raises(ValueError) as excinfo:
        Config(settings)
    assert str(excinfo.value) == "Config is not valid, a dbname is needed"


def test_file_without_password():
    settings = get_file("settings_without_password.json")
    with pytest.raises(ValueError) as excinfo:
        Config(settings)
    assert str(excinfo.value) == "Config is not valid, a password is needed"


def test_file_without_user():
    settings = get_file("settings_without_user.json")
    with pytest.raises(ValueError) as excinfo:
        Config(settings)
    assert str(excinfo.value) == "Config is not valid, a user is needed"
