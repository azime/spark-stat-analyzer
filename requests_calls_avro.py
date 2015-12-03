from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode, coalesce, lit
from pyspark.sql.types import ArrayType
from datetime import datetime, date, timedelta
import sys
import psycopg2
import calendar
import os
import shutil


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit("Missing arguments. Usage: " + sys.argv[0] + " <date> <source_root>")

    treatment_day = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    source_root = sys.argv[2]
    #source_root = '/home/vlepot/dev/navitia-stat-logger/tmp'
    #source_root = 'gs://hdp_test'

    print "Go for date: " + treatment_day.strftime('%Y-%m-%d')
    print "Source root dir: " + source_root

    conf = SparkConf().setAppName("requests_calls_compiler")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    treatment_day = date(2015, 11, 20)

    source_root = '/home/vlepot/dev/navitia-stat-logger/tmp'

    statsRows = sqlContext.read.format("com.databricks.spark.avro").load(source_root + '/' + treatment_day.strftime('%Y/%m/%d') + '/*.avro')

    print statsRows.count()
    print statsRows.first()

    requests_calls = statsRows.select(
        explode(statsRows['coverages.region_id']).alias('region_id'),
        statsRows['api'],
        statsRows['user_id'],
        statsRows['application_name'],
        statsRows['user_name'],
        statsRows['request_date'],
        statsRows['end_point_id'],
        statsRows['journeys']
    ).map(
        lambda row: (
            (
                row.region_id if row.region_id is not None else '',
                row.api,
                row.user_id,
                row.application_name,
                1 if 'canaltp.fr' in row.user_name else 0,
                datetime.utcfromtimestamp(row.request_date).date(),
                row.end_point_id,
            ),
            (
                1,
                1 if row.journeys is None or len(row.journeys) == 0 else 0,
                0
            )
        )
    ).reduceByKey(
        lambda (a, b, c), (d, e, f): (a+d, b+e, c+f)
    ).map(
        lambda tuple_of_tuples: [str(element) for tupl in tuple_of_tuples for element in tupl]
    ).map(
        lambda line: ";".join(line)
    )

    # print requests_calls.count()

    # Store on FS

    compiled_storage_rootdir = source_root + "/compiled/" + treatment_day.strftime('%Y/%m/%d')
    compiled_storage_dir = compiled_storage_rootdir + "/request_calls_" + treatment_day.strftime('%Y%m%d')

    # if os.path.isdir(compiled_storage_dir):
    #     shutil.rmtree(compiled_storage_dir)
    # else:
    #     if not os.path.isdir(compiled_storage_rootdir):
    #         os.makedirs(compiled_storage_rootdir)

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
