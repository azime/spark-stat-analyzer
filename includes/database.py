import psycopg2
from includes.utils import sub_iterable


class Database(object):
    def __init__(self, dbname, user, password, host="localhost", port=5432, auto_connect=True, **kwargs):
        self.connection_string = "host='{host}' port='{port}' dbname='{dbname}' user='{user}' password='{password}'".\
            format(host=host, port=port, dbname=dbname, user=user, password=password)
        self.schema = kwargs.get("schema", "stat_compiled")
        self.count = kwargs.get("count", 100)
        self.connection = None
        self.cursor = None
        if auto_connect:
            try:
                self.connect()
            except psycopg2.OperationalError:
                raise

    def connect(self):
        if not self.cursor:
            self.connection = psycopg2.connect(self.connection_string)
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

    def format_insert_query(self, table_name, columns, data):
        return "INSERT INTO {schema_}.{tablename} ({columns}) VALUES {template}".\
            format(schema_=self.schema, tablename=table_name, columns=", ".join(columns),
                   template=','.join(['%s'] * len(data)))

    def delete_by_date(self, tablename, start_date, end_date):
        query = "DELETE FROM {schema_}.{tablename} WHERE request_date >= ('{start_date}' :: date) " \
                "AND request_date < ('{end_date}' :: date) + interval '1 day'".format(tablename=tablename,
                                                                                      start_date=start_date,
                                                                                      end_date=end_date,
                                                                                      schema_=self.schema)
        self.execute(query)

    def select_from_table(self, tablename, columns, **where):
        query = "SELECT {columns} FROM {schema_}.{tablename}".format(columns=",".join(columns),
                                                                     tablename=tablename,
                                                                     schema_=self.schema)
        self.cursor.execute(query)
        return [tuple(values) for values in self.cursor.fetchall()]

    def insert(self, table_name, columns, data):
        for records in sub_iterable(data, self.count):
            if len(records):
                insert_string = self.format_insert_query(table_name, columns, records)
                self.execute(insert_string, records)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()
