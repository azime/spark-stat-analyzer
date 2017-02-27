import os
from datetime import datetime
from analyzers import AnalyseUsersSql, AnalyzeTokens, AnalyzeRequest,\
    AnalyzeErrors, AnalyzeCoverageStopAreas, AnalyzeCoverageModes, AnalyzeCoverageJourneysTransfers,\
    AnalyzeCoverageJourneysRequestsParams, AnalyzeCoverageNetworks, AnalyzeCoverageJourneys
from itertools import chain, islice


def analyzer_value(value):
    analyzers = {
        "token_stats": AnalyzeTokens,
        "users": AnalyseUsersSql,
        "requests_calls": AnalyzeRequest,
        "error_stats": AnalyzeErrors,
        "coverage_stop_areas": AnalyzeCoverageStopAreas,
        "coverage_modes": AnalyzeCoverageModes,
        "coverage_journeys_transfers": AnalyzeCoverageJourneysTransfers,
        "coverage_journeys_requests_params": AnalyzeCoverageJourneysRequestsParams,
        "coverage_journeys": AnalyzeCoverageJourneys,
        "coverage_networks": AnalyzeCoverageNetworks
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


def sub_iterable(iterable, size, format=tuple):
    it = iter(iterable)
    while True:
        yield format(chain((next(it),), islice(it, size - 1)))
