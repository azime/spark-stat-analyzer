import sys, os
sys.path.append(os.path.abspath("includes"))
from datetime import datetime
from time import time
from includes import common
import psycopg2

start = time()

(source_root, treatment_day_start, treatment_day_end) = common.get_period_from_input()
spark = common.start_spark_session(__file__)
file_list = common.get_file_list(source_root, treatment_day_start, treatment_day_end)
statLines = common.load_rdd_data(spark, file_list)
new_users = common.get_rdd_loaded_as_dict(statLines)

new_users = new_users.map(
    # keep useful data
    lambda dict: (dict['user_id'], dict['user_name'], dict['request_date'])
).map(
    # format to reduce by key
    lambda (user_id, user_name, request_date): (user_id, (user_name, request_date))
)

# take oldest date of new user
new_users_oldest_date = new_users.reduceByKey(
    lambda a, b: a if a[1] < b[1] else b
)

# take newest name of new user
new_users_newest_name = new_users.reduceByKey(
    lambda a, b: a if a[1] > b[1] else b
)

new_users_newest_name = new_users_newest_name.collect()
new_users_oldest_date = {id: date for (id, (name, date)) in new_users_oldest_date.collect()}

if len(new_users_newest_name) != 0:
    conn = psycopg2.connect(common.get_db_connection_string())
    cur = conn.cursor()
    cur.execute("SELECT id, user_name FROM stat_compiled.users")
    users_names_ids = {}
    for (id, name) in cur.fetchall():
        users_names_ids[id] = name

    rows = []
    for (user_id, (user_name, timestamp_request)) in new_users_newest_name:
        if user_id in users_names_ids.keys():
            if users_names_ids[user_id] != user_name:
                updateString = """
                UPDATE stat_compiled.users SET user_name=%s WHERE id=%s;
                """
                # print(updateString)
                # print((user_name, user_id))
                cur.execute(updateString, (user_name, user_id))
        else:
            insertString = """
            INSERT INTO stat_compiled.users (id, user_name, date_first_request)
            VALUES (%s, %s, %s);
            """
            # print(user_id, user_name, new_users_oldest_date[user_id])
            # print(insertString)
            cur.execute(insertString, (user_id, user_name, datetime.utcfromtimestamp(new_users_oldest_date[user_id])))


    cur.close()
    conn.commit()
    conn.close()
common.terminate(spark.sparkContext)
common.log_analyzer_stats("CanalTP\StatCompiler\Updater\UsersUpdater", treatment_day_start, treatment_day_end, start)
