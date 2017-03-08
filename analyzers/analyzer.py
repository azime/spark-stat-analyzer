from abc import abstractmethod, ABCMeta
from glob import glob
from datetime import timedelta, datetime
import math
from includes.logger import get_logger
import json
import os


class Analyzer(object):
    __metaclass__ = ABCMeta

    def __init__(self, storage_path, start_date, end_date, spark_session, database, **kwargs):
        self.storage_path = storage_path
        self.start_date = start_date
        self.end_date = end_date
        self.database = database
        self.spark_session = spark_session
        self.created_at = kwargs.get("current_datetime", datetime.now())

    @abstractmethod
    def launch(self):
        pass

    @property
    @abstractmethod
    def analyzer_name(self):
        pass

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        pass

    def collect_data(self, dataframe):
        data = dataframe.flatMap(
            self.get_tuples_from_stat_dict
        ).reduceByKey(
            lambda a, b: a + b
        ).collect()
        return [tuple(list(key_tuple) + [nb]) for (key_tuple, nb) in data]

    def get_data(self, rdd_mode=False, separator=','):
        files = self.get_files_to_analyze()
        df = self.load_data(files, rdd_mode, separator)
        return self.collect_data(df)

    def get_files_to_analyze(self):
        treatment_day = self.start_date
        file_list = []
        while treatment_day <= self.end_date:
            file_path = glob(os.path.join(self.storage_path, treatment_day.strftime('%Y/%m/%d'), "*.json.log*"))
            if self.storage_path .startswith("/") and len(file_path) > 0:
                file_list.extend(file_path)
            treatment_day += timedelta(days=1)
        return file_list

    def load_data(self, files, rdd_mode=False, separator=','):
        if rdd_mode:
            return self.spark_session.sparkContext.textFile(separator.join(files)).map(
                # json to dict
                lambda stat: json.loads(stat)
            )
        else:
            return self.spark_session.read.json(files)

    def get_log_analyzer_stats(self, current_datetime, status='OK'):
        return "[%s] [%s] [%s] [%s] [%d]" %(status, current_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                                            self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                                            self.analyzer_name,
                                            math.floor((current_datetime - self.created_at).total_seconds()))

    def terminate(self, current_datetime, status='OK'):
        self.spark_session.sparkContext.stop()
        get_logger().info(self.get_log_analyzer_stats(current_datetime, status))
