from abc import abstractmethod, ABCMeta
from glob import glob
from datetime import timedelta
import inspect
from datetime import datetime


class Analyzer(object):
    __metaclass__ = ABCMeta

    def __init__(self, storage_path, start_date, end_date, spark_context, database,
                 created_at=datetime.now()):
        self.storage_path = storage_path
        self.start_date = start_date
        self.end_date = end_date
        self.database = database
        self.spark_context = spark_context
        self.created_at = created_at

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

    def get_log_analyzer_stats(self, now=datetime.now()):
        runtime = now - self.created_at
        return "[spark-stat-analyzer] [OK] [%s] [%s] [%s] [%s]" %(now.strftime("%Y-%m-%d %H:%M:%S"),
                                                                  self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                                                                  inspect.getmro(self.__class__)[0].__name__,
                                                                  str(timedelta(seconds=runtime.seconds)))

    def terminate(self):
        self.spark_context.sparkContext.stop()
        print(self.log_analyzer_stats())
