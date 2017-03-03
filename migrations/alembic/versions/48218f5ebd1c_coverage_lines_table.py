"""coverage lines table

Revision ID: 48218f5ebd1c
Revises: 1cc79244cdf
Create Date: 2017-03-03 15:27:37.720878

"""

# revision identifiers, used by Alembic.
revision = '48218f5ebd1c'
down_revision = '1cc79244cdf'

from alembic import op
import sqlalchemy as sa
import geoalchemy2 as ga
import config

table_name = 'coverage_journeys'


def upgrade():
    op.create_table('coverage_lines',
    sa.Column('request_date', sa.DateTime(), nullable=False),
    sa.Column('region_id', sa.Text(), nullable=False),
    sa.Column('type', sa.Text(), nullable=False),
    sa.Column('line_id', sa.Text(), nullable=False),
    sa.Column('line_code', sa.Text(), nullable=False),
    sa.Column('network_id', sa.Text(), nullable=False),
    sa.Column('network_name', sa.Text(), nullable=False),
    sa.Column('is_internal_call', sa.SmallInteger(), nullable=False),
    sa.Column('nb', sa.BigInteger(), nullable=True),
    sa.PrimaryKeyConstraint('request_date', 'region_id', 'type', 'line_id', 'line_code', 'network_id', 'network_name', 'is_internal_call'),
    sa.UniqueConstraint('request_date', 'region_id', 'type', 'line_id', 'line_code', 'network_id', 'network_name', 'is_internal_call', 'nb', name='{schema}_{table}coverage_journeys_pkey'.format(schema=config.db['schema'], table=table_name)),
    schema='stat_compiled'
    )


def downgrade():
    op.drop_table('coverage_lines', schema='stat_compiled')
