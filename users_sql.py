import sys, os
sys.path.append(os.path.abspath("includes"))
from datetime import datetime
from time import time
from includes import common
import psycopg2
from pyspark.sql.window import Window
from pyspark.sql.functions import first, desc

start = time()

(source_root, treatment_day_start, treatment_day_end) = common.get_period_from_input()
spark = common.start_spark_session(__file__)
file_list = common.get_file_list(source_root, treatment_day_start, treatment_day_end)
df = common.get_sql_data_frame(spark, file_list)

paritionByUserId = Window.partitionBy("user_id")
wasc = paritionByUserId.orderBy("request_date")
wdesc = paritionByUserId.orderBy(desc("request_date"))

new_users = df\
    .select(
        "user_id",
        first('user_name').over(wdesc).alias('last_user_name'),
        first('request_date').over(wasc).alias('first_date')
    )\
    .distinct()

new_users = new_users.collect()
if len(new_users) != 0:
    rows = []
    conn = psycopg2.connect(common.get_db_connection_string())
    cur = conn.cursor()
    cur.execute("SELECT id, user_name FROM stat_compiled.users")
    users_names_ids = {}
    for (id, name) in cur.fetchall():
        users_names_ids[id] = name

    for new_users_row in new_users:
        if new_users_row.user_id in users_names_ids.keys():
            if users_names_ids[new_users_row.user_id] != new_users_row.last_user_name:
                updateString = """
                    UPDATE stat_compiled.users SET user_name=%s WHERE id=%s;
                    """
                # print(updateString)
                # print((new_users_row.last_user_name, new_users_row.user_id))
                cur.execute(updateString, (new_users_row.last_user_name, new_users_row.user_id))
        else:
            insertString = """
                INSERT INTO stat_compiled.users (id, user_name, date_first_request)
                VALUES (%s, %s, %s);
                """
            # print(new_users_row.user_id, new_users_row.last_user_name, new_users_row.first_date)
            # print(insertString)
            cur.execute(insertString, (new_users_row.user_id, new_users_row.last_user_name, datetime.utcfromtimestamp(new_users_row.first_date)))
    cur.close()
    conn.commit()
    conn.close()
common.terminate(spark.sparkContext)
common.log_analyzer_stats("CanalTP\StatCompiler\Updater\UsersUpdater", treatment_day_start, treatment_day_end, start)
