import os
from datetime import datetime
from analyzers import token_stat, users_sql, requests_calls


def analyzer_value(value):
    analyzers = {
        "token_stat": token_stat.AnalyzeToken,
        "users_sql": users_sql.AnalyseUsersSql,
        "requests_calls": requests_calls.AnalyzeRequest
    }
    lower_value = value.lower()
    if lower_value not in analyzers:
        error = "The {} argument must be in list {}, you gave {}".\
            format(value, str(analyzers.keys()), value)
        raise ValueError(error)
    return analyzers[lower_value]


def check_and_get_path(path):
    if not os.path.exists(path):
        raise NotImplementedError('Path not exist, you give {path}'.format(path=path))
    return path


def date_format(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError("Unable to parse date, {}".format(e))
