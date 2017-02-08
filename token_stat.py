import sys, os
sys.path.append(os.path.abspath("includes"))
from time import time
from includes import common
from pyspark.sql.functions import to_date

def process(df):
    dfProcessed = df.withColumn('request_date_ts', df.request_date.cast('timestamp'))
    tokenStats = dfProcessed.groupBy(to_date('request_date_ts').alias('request_date_trunc'), 'token').count()
    tokenStats = tokenStats.collect()
    # tokenRow attributes can be accessed by .token, .request_date_trunc but not .count which returns count method of tuple
    return [(tokenRow['token'], tokenRow['request_date_trunc'], tokenRow['count']) for tokenRow in tokenStats]

if __name__ == "__main__":

    start = time()

    (source_root, treatment_day_start, treatment_day_end) = common.get_period_from_input()
    spark = common.start_spark_session(__file__)
    file_list = common.get_file_list(source_root, treatment_day_start, treatment_day_end)

    df = common.get_sql_data_frame(spark, file_list)
    tokenStats = process(df)

    if len(tokenStats) != 0:
        conn = common.truncate_table_for_dates("token_stats", (treatment_day_start, treatment_day_end))
        common.insert_data_into_db("token_stats", ["token", "request_date", "nb_req"], tokenStats, conn)

    common.terminate(spark.sparkContext)
    common.log_analyzer_stats("TokenStatsUpdater", treatment_day_start, treatment_day_end, start)
