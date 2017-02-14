import psycopg2


class Database(object):
    def __init__(self, dbname, user, password, host="localhost", port=5432, auto_connect=True, **kwargs):
        self.connection_string = "host='{host}' port='{port}' dbname='{dbname}' user='{user}' password='{password}'".\
            format(host=host, port=port, dbname=dbname, user=user, password=password)
        self.schema = kwargs.get("schema", "stat_compiled")
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

    def insert(self, table_name, columns, rows):
        columns_as_string = ", ".join(columns)
        records_list_template = ','.join(['%s'] * len(rows))
        insert_string = "INSERT INTO stat_compiled.{0} ({1}) VALUES {2}".format(
            table_name, columns_as_string, records_list_template
        )

        try:
            self.cursor.execute(insert_string, rows)
            self.connection.commit()
        except psycopg2.Error as e:
            self.connection.rollback()
            print("""
                ERROR: Unable to insert into table %s (%s) rows:
                %s
                Exception:
                %s
            """ % (table_name, columns_as_string, rows, e))

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()
