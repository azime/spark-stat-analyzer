from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import first, desc
from analyzers import Analyzer
from includes.logger import get_logger


class AnalyseUsersSql(Analyzer):

    def collect_data(self, dataframe):
        if dataframe.count():
            partition_by_user_id = Window.partitionBy("user_id")
            wasc = partition_by_user_id.orderBy("request_date")
            wdesc = partition_by_user_id.orderBy(desc("request_date"))

            new_users = dataframe \
                .select(
                    "user_id",
                    first('user_name').over(wdesc).alias('last_user_name'),
                    first('request_date').over(wasc).alias('first_date')
                ) \
                .distinct()

            return new_users.collect()
        else:
            get_logger().debug("Empty data frame.")
            return []

    def insert_or_update(self, data):
        users_in_database = dict(self.database.select_from_table("users", ["id", "user_name"]))
        insert_values = []
        for d in data:
            if d.user_id in users_in_database:
                if d.last_user_name != users_in_database[d.user_id]:
                    self.database.update("UPDATE {schema_}.users SET user_name=%s WHERE id=%s;",
                                         (d.last_user_name, d.user_id))
            else:
                insert_values.append((d.user_id, d.last_user_name, datetime.utcfromtimestamp(d.first_date)))
        self.database.insert(table_name="users",
                             columns=("id", "user_name", "date_first_request"),
                             data=insert_values,
                             delete=False)

    def launch(self):
        users = self.get_data()
        self.insert_or_update(users)

    @property
    def analyzer_name(self):
        return "UsersUpdater"
