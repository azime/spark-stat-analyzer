from pyspark.sql.functions import to_date
from analyzer import Analyzer


class AnalyzeToken(Analyzer):

    def prepare_data(self, dataframe):
        dfProcessed = dataframe.withColumn('request_date_ts', dataframe.request_date.cast('timestamp'))
        tokenStats = dfProcessed.groupBy(to_date('request_date_ts').alias('request_date_trunc'), 'token').count()
        tokenStats = tokenStats.collect()
        # tokenRow attributes can be accessed by .token, .request_date_trunc but not .count which returns count method of tuple
        return [(tokenRow['token'], tokenRow['request_date_trunc'], tokenRow['count']) for tokenRow in tokenStats]

    def get_data(self):
        files = self.get_files_to_analyze()
        df = self.spark_context.read.json(files)
        return self.prepare_data(df)

    def truncate_and_insert(self, data):
        if len(data):
            self.database.delete_by_date("token_stats", self.start_date, self.end_date)
            query = "INSERT INTO stat_compiled.token_stats (token, request_date, nb_req) VALUES "
            self.database.execute(query, data)

    def launch(self):
        token_stats = self.get_data()
        self.truncate_and_insert(token_stats)
