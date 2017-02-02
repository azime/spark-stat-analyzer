import sys, os
sys.path.append(os.path.abspath("includes"))
from time import time
from includes import common
from pyspark.sql.functions import when, from_unixtime, lit

start = time()

(source_root, treatment_day_start, treatment_day_end) = common.get_period_from_input()
spark = common.start_spark_session(__file__)
file_list = common.get_file_list(source_root, treatment_day_start, treatment_day_end)
df = common.get_sql_data_frame(spark, file_list)

requests_calls = df.select(
    when(df.coverages[0].region_id.isNull(), '').otherwise(df.coverages[0].region_id).alias('region_id'),
    df.api,
    df.user_id,
    df.application_name,
    when(df.user_name.like('%canaltp%'), 1).otherwise(0).alias('is_internal_call'),
    from_unixtime(df.request_date, 'yyyy-MM-dd').alias('request_date'),
    when(df.end_point_id.isNull() , 1).when(df.end_point_id == 0, 1).otherwise(df.end_point_id).alias('end_point_id'),
    when(df.journeys.isNull(), 1).otherwise(0).alias('nb_without_journey'),
    when(df.info_response.isNull(), 0).otherwise(when(df.info_response.object_count.isNull(), 0).otherwise(df.info_response.object_count)).alias('object_count'),
).withColumn("nb", lit(1))\
    .groupBy("region_id", "api", "user_id", "application_name", "is_internal_call", "request_date", "end_point_id").agg({"nb": "sum", "nb_without_journey": "sum", "object_count": "sum"})

requests_calls_collection = requests_calls.collect()
request_calls_array = [(
                        requests_calls_row.region_id,
                        requests_calls_row.api,
                        requests_calls_row.user_id,
                        requests_calls_row.application_name,
                        requests_calls_row.is_internal_call,
                        requests_calls_row.request_date,
                        requests_calls_row.end_point_id,
                        requests_calls_row["sum(nb)"],
                        requests_calls_row["sum(nb_without_journey)"],
                        requests_calls_row["sum(object_count)"]
                       ) for requests_calls_row in requests_calls_collection ]

#DB insert
if len(request_calls_array) != 0:
    conn = common.truncate_table_for_dates("requests_calls", (treatment_day_start, treatment_day_end))
    table_cols = ["region_id", "api", "user_id", "app_name", "is_internal_call", "request_date", "end_point_id", "nb", "nb_without_journey", "object_count"]
    common.insert_data_into_db("requests_calls", table_cols, request_calls_array, conn)

common.terminate(spark.sparkContext)
common.log_analyzer_stats("CanalTP\StatCompiler\Updater\RequestCallsUpdater", treatment_day_start, treatment_day_end, start)
