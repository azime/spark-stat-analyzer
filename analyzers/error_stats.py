from pyspark.sql.functions import when, from_unixtime
from analyzers.stat_utils import region_id, is_internal_call, request_date
from analyzers import Analyzer


class AnalyzeErrors(Analyzer):
    def collect_data(self, df):
        return df.select(
                when(df['coverages'][0]['region_id'].isNull(), '').otherwise(df['coverages'][0]['region_id']).
                alias('region_id'),
                df['api'],
                from_unixtime(df['request_date'], 'yyyy-MM-dd').alias('request_date'),
                df['user_id'],
                df['application_name'],
                df['error']['id'].alias('err_id'),
                when(df['user_name'].like('%canaltp%'), 1).otherwise(0).alias('is_internal_call'),
            ).where(df['error'].isNotNull())\
            .groupBy('region_id', 'api', 'request_date', 'user_id', 'application_name', 'err_id', 'is_internal_call')\
            .count()\
            .collect()

    def truncate_and_insert(self, data):
        columns = ('region_id', 'api', 'request_date', 'user_id', 'app_name', 'err_id',
                   'is_internal_call', 'nb_req')
        self.database.insert(table_name="error_stats", columns=columns, data=data,
                             start_date=self.start_date,
                             end_date=self.end_date)

    def launch(self):
        error_stats = self.get_data()
        self.truncate_and_insert(error_stats)

    @property
    def analyzer_name(self):
        return "ErrorStatsUpdater"
