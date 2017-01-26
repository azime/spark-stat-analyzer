import sys, os, json
sys.path.append(os.path.abspath("includes"))
from datetime import datetime
from time import time
from includes import common
import psycopg2

start = time()
(sc, new_users, treatment_day_start, treatment_day_end) = common.get_data_from_input(__file__)

conn = psycopg2.connect(common.get_db_connection_string())
cur = conn.cursor()
cur.execute("SELECT id FROM stat_compiled.users")
users_ids = tuple(item[0] for item in cur.fetchall())

cur.close()
conn.commit()
conn.close()

# print(users_ids)

new_users = new_users.map(
    # keep usefull data
    lambda dict: (dict['user_id'], dict['user_name'], dict['request_date'])
).filter(
    # remove users already in database
    lambda (user_id, user_name, request_date), users_ids=users_ids: user_id not in users_ids
).map(
    # format to reduce by key
    lambda (user_id, user_name, request_date): (user_id, (user_name, request_date))
)

# take oldest date of new user
new_users_oldest_date = new_users.reduceByKey(
    lambda a, b: a if a[1] < b[1] else b
)

# print('=========================')
# print(new_users_oldest_date.count())
# print(new_users_oldest_date.collect())
# print('=========================')

new_users_oldest_date_collected = new_users_oldest_date.collect()

common.terminate(sc)

if len(new_users_oldest_date_collected) != 0:
    rows = []
    for (id_oldest, (name_oldest, timestamp_oldest)) in new_users_oldest_date_collected:
        rows.append((id_oldest, name_oldest, datetime.utcfromtimestamp(timestamp_oldest)))
    columns = ("id", "user_name", "date_first_request")
    common.insert_data_into_db("users", columns, rows)
    # print(rows)

common.log_analyzer_stats("CanalTP\StatCompiler\Updater\UsersUpdater", treatment_day_start, treatment_day_end, start)