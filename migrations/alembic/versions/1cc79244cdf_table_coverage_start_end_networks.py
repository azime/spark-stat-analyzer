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


def upgrade():
    op.create_table(
        'coverage_start_end_networks',
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
                            name='{schema}_coverage_start_end_networks_pkey'.format(schema=config.db['schema'])),
        schema=config.db['schema']
    )


def downgrade():
    op.drop_table('coverage_start_end_networks', schema=config.db['schema'])