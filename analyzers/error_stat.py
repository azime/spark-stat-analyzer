from time import time
from includes import common
from pyspark.sql.functions import to_date, when, from_unixtime
from analyzer import Analyzer

class AnalyzeError(Analyzer):
    def collect_data_from_df(self, df):
        return df.select(
                when(df['coverages'][0]['region_id'].isNull(), '').otherwise(df['coverages'][0]['region_id']).alias('region_id'),
                df['api'],
                df['request_date'],
                df['user_id'],
                df['application_name'],
                df['error']['id'].alias('err_id'),
                when(df['user_name'].like('%canaltp%'), 1).otherwise(0).alias('is_internal_call'),
            ) \
            .where(df['error'].isNotNull()) \
            .groupBy('region_id', 'api', 'request_date', 'user_id', 'application_name', 'err_id', 'is_internal_call') \
            .count() \
            .collect();

    def get_data(self):
        files = self.get_files_to_analyze()
        df = self.spark_context.read.json(files)
        return self.collect_data_from_df(df)

    def truncate_and_insert(self, data):
        if len(data):
            self.database.delete_by_date("error_stats", self.start_date, self.end_date)
            query = "INSERT INTO stat_compiled.error_stats (region_id, api, request_date, user_id, application_name, err_id, user_name, nb_req) VALUES "
            self.database.execute(query, data)

    def launch(self):
        error_stats = self.get_data()
        print(error_stats)
        #self.truncate_and_insert(error_stats)

