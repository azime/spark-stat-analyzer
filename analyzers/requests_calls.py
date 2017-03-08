from analyzers import Analyzer
from pyspark.sql.functions import when, from_unixtime, lit


class AnalyzeRequest(Analyzer):

    def collect_data(self, dataframe):
        if "journeys" not in dataframe.columns:
            dataframe = dataframe.withColumn("journeys", lit(None))

        requests_calls = dataframe.select(
            when(dataframe.coverages[0].region_id.isNull(), '').
            otherwise(dataframe.coverages[0].region_id).alias('region_id'),
            dataframe.api,
            dataframe.user_id,
            dataframe.application_name,
            when(dataframe.user_name.like('%canaltp%'), 1).otherwise(0).alias('is_internal_call'),
            from_unixtime(dataframe.request_date, 'yyyy-MM-dd').alias('request_date'),
            when(dataframe.end_point_id.isNull(), 1).when(dataframe.end_point_id == 0, 1).
            otherwise(dataframe.end_point_id).alias('end_point_id'),
            when(dataframe.journeys.isNull(), 1).otherwise(0).alias('nb_without_journey'),
            when(dataframe.info_response.isNull(), 0).otherwise(when(dataframe.info_response.object_count.isNull(), 0).
                                                                otherwise(dataframe.info_response.object_count)).
            alias('object_count'),
        ).withColumn("nb", lit(1))\
            .groupBy("region_id", "api", "user_id", "application_name", "is_internal_call",
                     "request_date", "end_point_id").agg({"nb": "sum",
                                                          "nb_without_journey": "sum", "object_count": "sum"})

        requests_calls_collection = requests_calls.collect()

        return [(requests_calls_row.region_id,
                 requests_calls_row.api,
                 requests_calls_row.user_id,
                 requests_calls_row.application_name,
                 requests_calls_row.is_internal_call,
                 requests_calls_row.request_date,
                 requests_calls_row.end_point_id,
                 requests_calls_row["sum(nb)"],
                 requests_calls_row["sum(nb_without_journey)"],
                 requests_calls_row["sum(object_count)"]
                ) for requests_calls_row in requests_calls_collection]

    def truncate_and_insert(self, data):
        columns = ("region_id", "api", "user_id", "app_name", "is_internal_call", "request_date",
                   "end_point_id", "nb", "nb_without_journey", "object_count")
        self.database.insert(table_name="requests_calls", columns=columns, data=data, start_date=self.start_date,
                             end_date=self.end_date)

    def launch(self):
        requests_calls = self.get_data()
        self.truncate_and_insert(requests_calls)

    @property
    def analyzer_name(self):
        return "RequestCallsUpdater"
