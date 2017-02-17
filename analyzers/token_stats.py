from pyspark.sql.functions import to_date
from analyzer import Analyzer
from includes.logger import get_logger


class AnalyzeTokens(Analyzer):

    def collect_data_from_df(self, dataframe):
        if dataframe.count():
            dfProcessed = dataframe.withColumn('request_date_ts', dataframe.request_date.cast('timestamp'))
            tokenStats = dfProcessed.groupBy(to_date('request_date_ts').alias('request_date_trunc'), 'token').count()
            tokenStats = tokenStats.collect()
            # tokenRow attributes can be accessed by .token, .request_date_trunc but not .count which returns count method of tuple
            return [(tokenRow['token'], tokenRow['request_date_trunc'], tokenRow['count']) for tokenRow in tokenStats]
        else:
            get_logger().debug("Empty data frame.")
            return []

    def get_data(self):
        files = self.get_files_to_analyze()
        df = self.spark_session.read.json(files)
        return self.collect_data_from_df(df)

    def truncate_and_insert(self, data):
        if len(data):
            self.database.insert(table_name="token_stats",
                                 columns=("token", "request_date", "nb_req"),
                                 data=data, start_date=self.start_date, end_date=self.end_date)

    def launch(self):
        token_stats = self.get_data()
        self.truncate_and_insert(token_stats)

    @property
    def analyzer_name(self):
        return "TokenStatsUpdater"
