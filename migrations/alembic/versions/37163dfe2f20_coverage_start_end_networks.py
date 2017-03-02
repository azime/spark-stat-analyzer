"""coverage_start_end_networks

Revision ID: 37163dfe2f20
Revises: None
Create Date: 2017-03-01 18:52:23.332202

"""

revision = '37163dfe2f20'
down_revision = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'coverage_start_end_network',
        sa.Column('region_id', sa.Text(), nullable=False),
        sa.Column('start_network_id', sa.Text(), nullable=False),
        sa.Column('start_network_name', sa.Text(), nullable=False),
        sa.Column('end_network_id', sa.Text(), nullable=False),
        sa.Column('end_network_name', sa.Text(), nullable=False),
        sa.Column('request_date', sa.Date(), nullable=False),
        sa.Column('is_internal_call', sa.Integer(), nullable=False),
        sa.Column('nb', sa.BIGINT(), nullable=False),
        sa.PrimaryKeyConstraint('region_id', 'start_network_id', 'end_network_id', 'request_date', 'is_internal_call'),
        sa.UniqueConstraint('region_id', 'start_network_id', 'end_network_id', 'request_date', 'is_internal_call',
                            name='coverage_start_end_networks_pkey'),
        schema='stat_compiled'
    )
    context = op.get_context()
    connection = op.get_bind()

    if not context.dialect.has_table(connection.engine, table_name='users', schema="stat_compiled"):
        op.create_table(
            'users',
            sa.Column('id', sa.Text(), nullable=False),
            sa.Column('user_name', sa.Text(), nullable=True),
            sa.Column('date_first_request', sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('id', name='users_pkey'),
            schema='stat_compiled'
        )


def downgrade():
    op.drop_table('coverage_start_end_network', schema='stat_compiled')
