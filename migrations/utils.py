# returns the SQL command creating the insert trigger
# responsible of partitioning the table by month
def get_create_partition_sql_func(schema, table):
    return """
    CREATE OR REPLACE FUNCTION {table}_insert_trigger()
        RETURNS TRIGGER AS $$
        DECLARE
          schema VARCHAR(100);
          partition VARCHAR(100);
        BEGIN
          schema := '{schema}';
          partition := '{table}' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
          IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
            RAISE NOTICE 'A partition has been created %',partition;
            EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition ||
                    ' (
                      check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || '''
                              AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' ||
                    'INHERITS (' || schema || '.{table});';
          END IF;
          EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.{table}' || ' ' || quote_literal(NEW) || ').*;';
          RETURN NULL;
        END;
        $$
        LANGUAGE plpgsql;
    """.format(schema=schema, table=table)


def get_drop_partition_sql_func(table):
    return "DROP FUNCTION IF EXISTS {}_insert_trigger();".format(table)
