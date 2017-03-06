"""coverage lines table

Revision ID: 48218f5ebd1c
Revises: 1cc79244cdf
Create Date: 2017-03-03 15:27:37.720878

"""

# revision identifiers, used by Alembic.
revision = '48218f5ebd1c'
down_revision = '1cc79244cdf'

from alembic import op
from migrations.utils import get_create_partition_sql_func, get_drop_partition_sql_func, \
                             get_create_trigger_sql
import sqlalchemy as sa
import config

table_name = 'coverage_lines'
schema = config.db['schema']


def upgrade():
    op.create_table(table_name,
                    sa.Column('request_date', sa.DateTime(), nullable=False),
                    sa.Column('region_id', sa.Text(), nullable=False),
                    sa.Column('type', sa.Text(), nullable=False),
                    sa.Column('line_id', sa.Text(), nullable=False),
                    sa.Column('line_code', sa.Text(), nullable=False),
                    sa.Column('network_id', sa.Text(), nullable=False),
                    sa.Column('network_name', sa.Text(), nullable=False),
                    sa.Column('is_internal_call', sa.SmallInteger(), nullable=False),
                    sa.Column('nb', sa.BigInteger(), nullable=True),
                    sa.PrimaryKeyConstraint('request_date', 'region_id', 'type', 'line_id', 'line_code', 'network_id',
                                            'network_name', 'is_internal_call'),
                    sa.UniqueConstraint('request_date', 'region_id', 'type', 'line_id', 'line_code', 'network_id',
                                        'network_name', 'is_internal_call', name='{schema}_{table}_pkey'.
                                        format(schema=schema, table=table_name)),
                    schema=schema
    )

    op.execute(get_create_partition_sql_func(schema, table_name))
    op.execute(get_create_trigger_sql(schema, table_name))


def downgrade():
    op.drop_table(table_name, schema=schema)
    op.execute(get_drop_partition_sql_func(table_name))
