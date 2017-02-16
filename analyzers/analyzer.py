from abc import abstractmethod, ABCMeta
from glob import glob
from datetime import timedelta
from datetime import datetime
import math


class Analyzer(object):
    __metaclass__ = ABCMeta

    def __init__(self, storage_path, start_date, end_date, spark_context, database, **kwargs):
        self.storage_path = storage_path
        self.start_date = start_date
        self.end_date = end_date
        self.database = database
        self.spark_context = spark_context
        self.created_at = kwargs.get("current_datetime", datetime.now())

    def get_files_to_analyze(self):
        treatment_day = self.start_date
        file_list = []
        while treatment_day <= self.end_date:
            file_path = glob(self.storage_path + '/' + treatment_day.strftime('%Y/%m/%d') + '/*.json.log*')
            if self.storage_path .startswith("/") and len(file_path) > 0:
                file_list.extend(file_path)
            treatment_day += timedelta(days=1)
        return file_list

    @abstractmethod
    def launch(self):
        pass

    def get_log_analyzer_stats(self, current_datetime):
        return "[spark-stat-analyzer] [OK] [%s] [%s] [%s] [%d]" %(current_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                                                                  self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                                                                  self.analyzer_name,
                                                                  math.floor((current_datetime - self.created_at).total_seconds()))

    def terminate(self, current_datetime):
        self.spark_context.sparkContext.stop()
        print(self.get_log_analyzer_stats(current_datetime))

    @property
    @abstractmethod
    def analyzer_name(self):
        pass
