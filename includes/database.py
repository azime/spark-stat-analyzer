import psycopg2


class Database(object):
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.connection_string = "host='{host}' port='{port}' dbname='{dbname}' user='{user}' password='{password}'".\
            format(host=host, port=port, dbname=dbname, user=user, password=password)
        try:
            self.connection = psycopg2.connect(self.connection_string)
        except psycopg2.OperationalError:
            raise
        self.cursor = self.connection.cursor()

    def connect(self):
        if not self.cursor:
            self.cursor = self.connection.cursor()

    def execute(self, query, values=None):
        try:
            self.cursor.execute(query, values)
            self.commit()
        except psycopg2.Error:
            self.rollback()
        except TypeError:
            self.rollback()
            raise

    def delete_by_date(self, tablename, start_date, end_date):
        query = "DELETE FROM stat_compiled.{tablename} WHERE request_date >= ('{start_date}' :: date) " \
                "AND request_date < ('{end_date}' :: date) + interval '1 day'".format(tablename=tablename,
                                                                                      start_date=start_date,
                                                                                      end_date=end_date)
        self.execute(query)

    def select_from_table(self, tablename, columns, **where):
        query = "SELECT {columns} FROM stat_compiled.{tablename}".format(columns=",".join(columns), tablename=tablename)
        self.cursor.execute(query)
        return [tuple(values) for values in self.cursor.fetchall()]

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()