from datetime import datetime
from time import time
from includes import common
import psycopg2

def get_tuples_from_stat_dict(statDict):
    result = []
    if len(statDict['coverages']) > 0:
        for cov in statDict['coverages']:
            coverages = []
            region_id = cov['region_id'] if 'region_id' in cov else ''
            if not region_id in coverages:
                coverages.append(region_id)

        for cov in coverages:
            result.append(
                (
                    (
                        cov,  # region_id
                        statDict['api'],  # api
                        statDict['user_id'],  # user_id
                        statDict['application_name'],  # app_name
                        1 if 'canaltp' in statDict['user_name'] else 0,  # is_internal_call
                        datetime.utcfromtimestamp(statDict['request_date']).date(),  # request_date
                        statDict['end_point_id'] if 'end_point_id' in statDict and statDict['end_point_id'] != 0 else 1,  # end_point_id
                    ),
                    (
                        1,  # nb
                        1 if not 'journeys' in statDict or len(statDict['journeys']) == 0 else 0,  # nb_without_journey
                        statDict['info_response']['object_count'] if 'info_response' in statDict and 'object_count' in statDict['info_response'] else 0
                    )
                )
            )
    return result

start = time()

(source_root, treatment_day_start, treatment_day_end) = common.get_period_from_input()
spark = common.start_spark_session(__file__)
file_list = common.get_file_list(source_root, treatment_day_start, treatment_day_end)
statLines = common.load_rdd_data(spark, file_list)
dayStats = common.get_rdd_loaded_as_dict(statLines)
requests_calls = dayStats.flatMap(
    get_tuples_from_stat_dict
).reduceByKey(
    lambda (a, b, c), (d, e, f): (a+d, b+e, c+f)
).map(
    lambda tuple_of_tuples: [str(element) for tupl in tuple_of_tuples for element in tupl]
).map(
    lambda line: ";".join(line)
)


# DB insert

def insert_requests_calls(cursor, line):
    sql_query_fmt = """
    INSERT INTO stat_compiled.requests_calls
    (
        region_id, api, user_id, app_name, is_internal_call, request_date,
        end_point_id, nb, nb_without_journey, object_count
    )
    VALUES
    (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s
    )
    """
    cursor.execute(sql_query_fmt, line.split(';'))
conn = psycopg2.connect(common.get_db_connection_string())
cur = conn.cursor()
cur.execute("DELETE FROM stat_compiled.requests_calls WHERE request_date >= %s AND request_date <= %s", (treatment_day_start, treatment_day_end))
cur.close()
insert_cur = conn.cursor()
for requests_calls_line in requests_calls.collect():
    insert_requests_calls(insert_cur, requests_calls_line)
conn.commit()
insert_cur.close()
conn.close()
common.terminate(spark.sparkContext)
common.log_analyzer_stats("CanalTP\StatCompiler\Updater\RequestCallsUpdater", treatment_day_start, treatment_day_end, start)