"""table coverage_journeys

Revision ID: 1cc79244cdf
Revises: None
Create Date: 2017-03-02 11:11:06.697861

"""

# revision identifiers, used by Alembic.
revision = '1cc79244cdf'
down_revision = '22e888ef4fa'

from alembic import op
import sqlalchemy as sa
import config

table = "coverage_start_end_networks"


def upgrade():
    op.create_table(
        table,
        sa.Column('region_id', sa.Text(), nullable=False),
        sa.Column('start_network_id', sa.Text(), nullable=False),
        sa.Column('start_network_name', sa.Text(), nullable=False),
        sa.Column('end_network_id', sa.Text(), nullable=False),
        sa.Column('end_network_name', sa.Text(), nullable=False),
        sa.Column('request_date', sa.DateTime(), nullable=False),
        sa.Column('is_internal_call', sa.SmallInteger(), nullable=False),
        sa.Column('nb', sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint('region_id', 'start_network_id', 'end_network_id', 'request_date', 'is_internal_call'),
        sa.UniqueConstraint('region_id', 'start_network_id', 'end_network_id', 'request_date', 'is_internal_call',
                            name='{schema}_{table}_pkey'.format(schema=config.db['schema'], table=table)),
        schema=config.db['schema']
    )
    op.execute("""
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
            """.format(schema=config.db['schema'], table=table
        ))
    op.execute("""
            CREATE TRIGGER insert_{table}_trigger
                BEFORE INSERT ON {schema}.{table}
                FOR EACH ROW EXECUTE PROCEDURE {table}_insert_trigger();
            """.format(table=table, schema=config.db['schema']
        ))


def downgrade():
    op.drop_table(table, schema=config.db['schema'])
    op.execute("""DROP FUNCTION IF EXISTS {table}_insert_trigger();""".format(table=table))
