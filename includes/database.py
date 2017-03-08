import psycopg2
from includes.utils import sub_iterable
from includes.logger import get_logger


class Database(object):
    def __init__(self, dbname, user, password, host="localhost", port=5432, auto_connect=True, **kwargs):
        self.connection_string = "host='{host}' port='{port}' dbname='{dbname}' user='{user}' password='{password}'".\
            format(host=host, port=port, dbname=dbname, user=user, password=password)
        self.schema = kwargs.get("schema", "stat_compiled")
        self.insert_count = kwargs.get("insert_count", 1000)
        self.connection = None
        self.cursor = None
        if auto_connect:
            try:
                self.connect()
            except psycopg2.OperationalError as e:
                get_logger().critical('Cannot connect database, error: {msg}'.format(msg=str(e)))
                raise

    def connect(self):
        if not self.cursor:
            self.connection = psycopg2.connect(self.connection_string)
            self.cursor = self.connection.cursor()

    def format_insert_query(self, table_name, columns, data):
        return "INSERT INTO {schema_}.{tablename} ({columns}) VALUES {template}".\
            format(schema_=self.schema, tablename=table_name, columns=", ".join(columns),
                   template=','.join(['%s'] * len(data)))

    def format_delete_query(self, table_name, start_date, end_date):
        return "DELETE FROM {schema_}.{table_name} WHERE request_date >= ('{start_date}' :: date) " \
               "AND request_date < ('{end_date}' :: date) + interval '1 day'".format(table_name=table_name,
                                                                                     start_date=start_date,
                                                                                     end_date=end_date,
                                                                                     schema_=self.schema)

    def select_from_table(self, table_name, columns, **where):
        self.connect()
        query = "SELECT {columns} FROM {schema_}.{table_name}".format(columns=",".join(columns),
                                                                      table_name=table_name,
                                                                      schema_=self.schema)
        self.cursor.execute(query)
        return [tuple(values) for values in self.cursor.fetchall()]

    def update(self, query, values):
        try:
            self.connect()
            self.cursor.execute(query.format(schema_=self.schema), values)
            self.connection.commit()
        except psycopg2.Error as e:
            get_logger().critical("Error in update function: {msg}".format(msg=str(e)))
            self.connection.rollback()
            raise
        except TypeError as e:
            get_logger().critical("Error in update function: {msg}".format(msg=str(e)))
            self.connection.rollback()
            raise

    def insert(self, table_name, columns, data, start_date=None, end_date=None, delete=True):
        if not len(data):
            return
        try:
            self.connect()
            if delete:
                query = self.format_delete_query(table_name, start_date, end_date)
                self.cursor.execute(query)
            size = len(data)
            count = 0
            for records in sub_iterable(data, self.insert_count):
                if len(records):
                    count += len(records)
                    get_logger().info("Insert into {table} {count}/{size}".format(table=table_name,
                                                                                  count=count,
                                                                                  size=size))
                    insert_string = self.format_insert_query(table_name, columns, records)
                    self.cursor.execute(insert_string, records)
            self.connection.commit()
        except psycopg2.Error as e:
            get_logger().critical("Error in insert function: {msg}".format(msg=str(e)))
            self.connection.rollback()
            raise
        except TypeError as e:
            get_logger().critical("Error in insert function: {msg}".format(msg=str(e)))
            self.connection.rollback()
            raise
