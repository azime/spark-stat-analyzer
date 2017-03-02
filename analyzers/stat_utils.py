from datetime import datetime


def region_id(stat_dict):
    return stat_dict['coverages'][0]['region_id']


def is_internal_call(stat_dict):
    return 1 if 'canaltp' in stat_dict['user_name'] else 0


def request_date(stat_dict):
    return datetime.utcfromtimestamp(stat_dict['request_date']).date()
