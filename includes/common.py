import sys
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from glob import glob
from time import time
import math
import json
import psycopg2
import config


def get_period_from_input():
    if len(sys.argv) < 3:
        raise SystemExit("Missing arguments. Usage: " + sys.argv[0] + " <source_root> <start_date> <end_date> ")

    source_root = sys.argv[1]
    treatment_day_start = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    treatment_day_end = datetime.strptime(sys.argv[3], '%Y-%m-%d').date()

    if treatment_day_start > treatment_day_end:
        raise RuntimeError("<start_date> should be less than or equal to <end_date>")

    print "Go for dates: " + treatment_day_start.strftime('%Y-%m-%d') + " -> " + treatment_day_end.strftime('%Y-%m-%d')
    print "Source root dir: " + source_root

    return (source_root, treatment_day_start, treatment_day_end)


def get_file_list(source_root, treatment_day_start, treatment_day_end):
    treatment_day = treatment_day_start
    file_list = []
    while treatment_day <= treatment_day_end:
        if source_root.startswith("/") and len(
            glob(source_root + '/' + treatment_day.strftime('%Y/%m/%d') + '/*.json.log*')
        ) > 0:
            file_list = file_list + glob(source_root + '/' + treatment_day.strftime('%Y/%m/%d') + '/*.json.log*')
        treatment_day += timedelta(days=1)
    return file_list


def start_spark_session(analyzer_name):
    return SparkSession.builder \
        .appName(analyzer_name) \
        .getOrCreate()

def load_rdd_data(spark, file_list):
    sc = spark.sparkContext
    statsLines = sc.textFile(','.join(file_list))
    return statsLines


def get_rdd_loaded_as_dict(statsLines):
    return statsLines.map(
        # json to dict
        lambda stat: json.loads(stat)
    )

def get_sql_data_frame(spark, statsLines):
    return spark.read.json(statsLines)


def get_elapsed_time(start):
    end = time()
    runtime = end - start
    return math.floor(runtime)


def terminate(sc):
    sc.stop()


def get_db_connection_string():
    return "host='%s' port='%s' dbname='%s' user='%s' password='%s'" % (
        config.db['host'],
        config.db['port'],
        config.db['dbname'],
        config.db['user'],
        config.db['password']
    )


def log_analyzer_stats(analyzer, treatment_day_start, treatment_day_end, start_time):
    if treatment_day_start == treatment_day_end:
        duration = get_elapsed_time(start_time)
        print(
            "[spark-stat-analyzer] [OK] [%s] [%s] [%s] [%d]" %
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), treatment_day_start, analyzer, duration)
        )


def truncate_table_for_dates(table_name, period_dates, date_column_name="request_date"):
    conn = psycopg2.connect(get_db_connection_string())
    cur = conn.cursor()
    cur.execute("DELETE from stat_compiled.{0} where {1} >= ('{2}' :: date) and {1} < ('{3}' :: date) + interval '1 day'".format(
        table_name, date_column_name, period_dates[0], period_dates[1]
    ))
    cur.close()
    return conn


def insert_data_into_db(table_name, columns, rows, conn=None):

    if conn is None:
        conn = psycopg2.connect(get_db_connection_string())

    cur = conn.cursor()

    columns_as_string = ", ".join(columns)
    records_list_template = ','.join(['%s'] * len(rows))
    insert_string = "INSERT INTO stat_compiled.{0} ({1}) VALUES {2}".format(
        table_name, columns_as_string, records_list_template
    )

    try:
        cur.execute(insert_string, rows)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("""
ERROR: Unable to insert into table %s (%s) rows:
%s
Exception:
%s
        """ % (table_name, columns_as_string, rows, e))
    cur.close()
    conn.close()

