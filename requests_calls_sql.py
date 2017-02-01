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

df.select(
    when(df.coverages[0].region_id.isNull(), '').otherwise(df.coverages[0].region_id).alias('region_id'),
    df.api,
    df.user_id,
    df.application_name,
    when(df.user_name == 'canaltp', 1).otherwise(0).alias('is_internal_call'),
    from_unixtime(df.request_date, 'yyyy-MM-dd').alias('request_date'),
    when(df.end_point_id.isNull() , 1).when(df.end_point_id == 0, 1).otherwise(df.end_point_id).alias('end_point_id'),
    when(df.journeys.isNull(), 1).otherwise(0).alias('n_w_j'),
    when(df.info_response.isNull(), 0).otherwise(when(df.info_response.object_count.isNull(), 0).otherwise(df.info_response.object_count)).alias('o_c'),
).withColumn("nb", lit(1))\
    .groupBy("region_id", "api", "user_id", "application_name", "is_internal_call", "request_date", "end_point_id").agg({"nb": "sum", "n_w_j": "sum", "o_c": "sum"}).show()

# DB insert
#
# def insert_requests_calls(cursor, line):
#     sql_query_fmt = """
#     INSERT INTO stat_compiled.requests_calls
#     (
#         region_id, api, user_id, app_name, is_internal_call, request_date,
#         end_point_id, nb, nb_without_journey, object_count
#     )
#     VALUES
#     (
#         %s, %s, %s, %s, %s, %s,
#         %s, %s, %s, %s
#     )
#     """
#     cursor.execute(sql_query_fmt, line.split(';'))
# conn = psycopg2.connect(common.get_db_connection_string())
# cur = conn.cursor()
# cur.execute("DELETE FROM stat_compiled.requests_calls WHERE request_date >= %s AND request_date <= %s", (treatment_day_start, treatment_day_end))
# cur.close()
# insert_cur = conn.cursor()
# for requests_calls_line in requests_calls.collect():
#     insert_requests_calls(insert_cur, requests_calls_line)
# conn.commit()
# insert_cur.close()
# conn.close()
# common.terminate(spark.sparkContext)
# common.log_analyzer_stats("CanalTP\StatCompiler\Updater\RequestCallsUpdater", treatment_day_start, treatment_day_end, start)