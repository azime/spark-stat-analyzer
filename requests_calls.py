from pyspark import SparkContext, SparkConf
import json
from datetime import datetime, timedelta
import sys
#import psycopg2
import calendar
import os
import shutil
from glob import glob


def get_tuples_from_stat_dict(statDict):
    result = []
    if len(statDict['coverages']) > 0:
        for cov in statDict['coverages']:
            result.append(
                (
                    (
                        cov['region_id'] if 'region_id' in cov else '',  # region_id
                        statDict['api'],  # api
                        statDict['user_id'],  # user_id
                        statDict['application_name'],  # app_name
                        1 if 'canaltp.fr' in statDict['user_name'] else 0,  # is_internal_call
                        datetime.utcfromtimestamp(statDict['request_date']).date(),  # request_date
                        statDict['end_point_id'] if 'end_point_id' in statDict else 1,  # end_point_id
                        statDict['host']
                    ),
                    (
                        1,  # nb
                        1 if not 'journeys' in statDict or len(statDict['journeys']) == 0 else 0,  # nb_without_journey
                        statDict['info_response']['object_count'] if 'info_response' in statDict and 'object_count' in statDict['info_response'] else 0
                    )
                )
            )
    return result

if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit("Missing arguments. Usage: " + sys.argv[0] + " <date> <source_root>")

    #treatment_day = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    #source_root = '/home/vlepot/dev/navitia-stat-logger/tmp'
    #source_root = 'gs://hdp_test'

    source_root = sys.argv[1]
    treatment_day_start = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    treatment_day_end = datetime.strptime(sys.argv[3], '%Y-%m-%d').date()

    print "Go for dates: " + treatment_day_start.strftime('%Y-%m-%d') + " -> " + treatment_day_end.strftime('%Y-%m-%d')
    print "Source root dir: " + source_root

    conf = SparkConf().setAppName("requests_calls_compiler")
    sc = SparkContext(conf=conf)

    statsLines = sc.emptyRDD()
    treatment_day = treatment_day_start
    while treatment_day <= treatment_day_end:
        if source_root.startswith("/") and len(glob(source_root + '/' + treatment_day.strftime('%Y/%m/%d') + '/*.json.log*')) > 0:
            statsLines = statsLines.union(sc.textFile(source_root + '/' + treatment_day.strftime('%Y/%m/%d') + '/*.json.log*'))
        treatment_day += timedelta(days=1)

    dayStats = statsLines.map(
        lambda stat: json.loads(stat)
    )

    # print dayStats.first()
    # print dayStats.count()

    requests_calls = dayStats.flatMap(
        get_tuples_from_stat_dict
    ).reduceByKey(
        lambda (a, b, c), (d, e, f): (a+d, b+e, c+f)
    ).map(
        lambda tuple_of_tuples: [str(element) for tupl in tuple_of_tuples for element in tupl]
    ).map(
        lambda line: ";".join(line)
    )

    # print requests_calls.count()

    # Store on FS

    compiled_storage_rootdir = source_root + "/compiled/" + treatment_day_start.strftime('%Y/%m/%d')
    compiled_storage_dir = compiled_storage_rootdir + "/request_calls_" + treatment_day_start.strftime('%Y%m%d')

    if source_root.startswith('/'): # In case of local file system do some preparation first
        if os.path.isdir(compiled_storage_dir):
            shutil.rmtree(compiled_storage_dir)
        else:
            if not os.path.isdir(compiled_storage_rootdir):
                os.makedirs(compiled_storage_rootdir)

    requests_calls.saveAsTextFile(compiled_storage_dir)

    # DB insert

    #
    # def insert_requests_calls(cursor, line):
    #     sql_query_fmt = """
    #     INSERT INTO stat_compiled.requests_calls
    #     (
    #         region_id, api, user_id, app_name, is_internal_call, request_date,
    #         nb, nb_without_journey, end_point_id, object_count
    #     )
    #     VALUES
    #     (
    #         %s, %s, %s, %s, %s, %s,
    #         %s, %s, %s, %s
    #     )
    #     """
    #     cursor.execute(sql_query_fmt, line)
    #
    # conn_string = "host='statdb.docker' port='5432' dbname='statistics' user='stat_compiled' password='stat_compiled'"
    # conn = psycopg2.connect(conn_string)
    #
    # cur = conn.cursor()
    # cur.execute("DELETE FROM stat_compiled.requests_calls WHERE request_date = %s", (treatment_day,))
    # cur.close()
    #
    # insert_cur = conn.cursor()
    # for requests_calls_line in requests_calls.collect():
    #     insert_requests_calls(insert_cur, requests_calls_line)
    #
    # conn.commit()
    # insert_cur.close()
    # conn.close()
