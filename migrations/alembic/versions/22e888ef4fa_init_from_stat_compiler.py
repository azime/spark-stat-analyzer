"""init from stat-compiler

Revision ID: 22e888ef4fa
Revises: None
Create Date: 2017-03-02 15:20:51.315068

"""

# revision identifiers, used by Alembic.
revision = '22e888ef4fa'
down_revision = None

from alembic import op
import sqlalchemy as sa
import config


def upgrade():
    context = op.get_context()
    connection = op.get_bind()

    if not context.dialect.has_table(connection.engine, table_name='coverage_journeys',
                                     schema=config.db['schema']):
        op.create_table('coverage_journeys',
                        sa.Column('request_date', sa.DateTime(), nullable=False),
                        sa.Column('region_id', sa.Text(), nullable=False),
                        sa.Column('is_internal_call', sa.Boolean(), nullable=False),
                        sa.Column('nb', sa.BigInteger(), nullable=False),
                        sa.PrimaryKeyConstraint('request_date', 'region_id', 'is_internal_call'),
                        sa.UniqueConstraint('region_id', 'request_date', 'is_internal_call',
                                            name='{schema}_coverage_journeys_pkey'.format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )
    if not context.dialect.has_table(connection.engine, table_name='coverage_journeys_requests_params',
                                     schema=config.db['schema']):
        op.create_table('coverage_journeys_requests_params',
                        sa.Column('request_date', sa.DateTime(), nullable=False),
                        sa.Column('region_id', sa.Text(), nullable=False),
                        sa.Column('is_internal_call', sa.Boolean(), nullable=False),
                        sa.Column('nb_wheelchair', sa.BigInteger(), nullable=False),
                        sa.PrimaryKeyConstraint('request_date', 'region_id', 'is_internal_call'),
                        sa.UniqueConstraint('region_id', 'request_date', 'is_internal_call',
                                            name='{schema}_coverage_journeys_requests_params_pkey'.
                                            format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )
    if not context.dialect.has_table(connection.engine, table_name='coverage_journeys_transfers',
                                     schema=config.db['schema']):
        op.create_table('coverage_journeys_transfers',
                        sa.Column('request_date', sa.DateTime(), nullable=False),
                        sa.Column('region_id', sa.Text(), nullable=False),
                        sa.Column('is_internal_call', sa.Boolean(), nullable=False),
                        sa.Column('nb_transfers', sa.BigInteger(), nullable=False),
                        sa.Column('nb', sa.BigInteger(), nullable=True),
                        sa.PrimaryKeyConstraint('request_date', 'region_id', 'is_internal_call', 'nb_transfers'),
                        sa.UniqueConstraint('region_id', 'request_date', 'is_internal_call', 'nb_transfers',
                                            name='{schema}_coverage_journeys_transfers_pkey'.
                                            format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )
    if not context.dialect.has_table(connection.engine, table_name='coverage_modes', schema=config.db['schema']):
        op.create_table('coverage_modes',
                        sa.Column('request_date', sa.DateTime(), nullable=False),
                        sa.Column('region_id', sa.Text(), nullable=False),
                        sa.Column('type', sa.Text(), nullable=False),
                        sa.Column('mode', sa.Text(), nullable=False),
                        sa.Column('commercial_mode_id', sa.Text(), nullable=False),
                        sa.Column('commercial_mode_name', sa.Text(), nullable=False),
                        sa.Column('is_internal_call', sa.Boolean(), nullable=False),
                        sa.Column('nb', sa.BigInteger(), nullable=True),
                        sa.PrimaryKeyConstraint('request_date', 'region_id', 'type', 'mode',
                                                'commercial_mode_id', 'commercial_mode_name',
                                                'is_internal_call'),
                        sa.UniqueConstraint('region_id', 'request_date', 'is_internal_call', 'type', 'mode',
                                            'commercial_mode_id', 'commercial_mode_name',
                                            name='{schema}_coverage_modes_pkey'.format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )
    if not context.dialect.has_table(connection.engine, table_name='coverage_networks',
                                     schema=config.db['schema']):
        op.create_table('coverage_networks',
                        sa.Column('request_date', sa.DateTime(), nullable=False),
                        sa.Column('region_id', sa.Text(), nullable=False),
                        sa.Column('network_id', sa.Text(), nullable=False),
                        sa.Column('network_name', sa.Text(), nullable=False),
                        sa.Column('is_internal_call', sa.Boolean(), nullable=False),
                        sa.Column('nb', sa.BigInteger(), nullable=True),
                        sa.PrimaryKeyConstraint('request_date', 'region_id', 'network_id', 'network_name',
                                                'is_internal_call'),
                        sa.UniqueConstraint('region_id', 'request_date', 'is_internal_call', 'network_id',
                                            'network_name',
                                            name='{schema}_coverage_networks_pkey'.format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )
    if not context.dialect.has_table(connection.engine, table_name='coverage_stop_areas',
                                     schema=config.db['schema']):
        op.create_table('coverage_stop_areas',
                        sa.Column('request_date', sa.DateTime(), nullable=False),
                        sa.Column('region_id', sa.Text(), nullable=False),
                        sa.Column('stop_area_id', sa.Text(), nullable=False),
                        sa.Column('stop_area_name', sa.Text(), nullable=False),
                        sa.Column('city_id', sa.Text(), nullable=False),
                        sa.Column('city_name', sa.Text(), nullable=False),
                        sa.Column('city_insee', sa.Text(), nullable=False),
                        sa.Column('department_code', sa.Text(), nullable=False),
                        sa.Column('is_internal_call', sa.Boolean(), nullable=False),
                        sa.Column('nb', sa.BigInteger(), nullable=False),
                        sa.PrimaryKeyConstraint('request_date', 'region_id', 'stop_area_id', 'stop_area_name',
                                                'city_id', 'city_name', 'city_insee', 'department_code',
                                                'is_internal_call', 'nb'),
                        sa.UniqueConstraint('region_id', 'request_date', 'is_internal_call', 'stop_area_id',
                                            'stop_area_name', 'city_id', 'city_name', 'city_insee',
                                            'department_code',
                                            name='{schema}_coverage_stop_areas_pkey'.format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )
    if not context.dialect.has_table(connection.engine, table_name='error_stats', schema=config.db['schema']):
        op.create_table('error_stats',
                        sa.Column('region_id', sa.Text(), nullable=False),
                        sa.Column('api', sa.Text(), nullable=False),
                        sa.Column('user_id', sa.Integer(), nullable=False),
                        sa.Column('app_name', sa.Text(), nullable=False),
                        sa.Column('is_internal_call', sa.Boolean(), nullable=False),
                        sa.Column('request_date', sa.DateTime(), nullable=False),
                        sa.Column('err_id', sa.Text(), nullable=False),
                        sa.Column('nb_req', sa.BigInteger(), nullable=True),
                        sa.Column('nb_without_journey', sa.BigInteger(), nullable=True),
                        sa.PrimaryKeyConstraint('region_id', 'api', 'user_id', 'app_name', 'is_internal_call',
                                                'request_date', 'err_id'),
                        sa.UniqueConstraint('region_id', 'request_date', 'is_internal_call', 'api', 'err_id',
                                            'app_name', 'user_id',
                                            name='{schema}_error_stats_pkey'.format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )
    if not context.dialect.has_table(connection.engine, table_name='requests_calls',
                                     schema=config.db['schema']):
        op.create_table('requests_calls',
                        sa.Column('region_id', sa.Text(), nullable=False),
                        sa.Column('api', sa.Text(), nullable=False),
                        sa.Column('user_id', sa.Integer(), nullable=False),
                        sa.Column('app_name', sa.Text(), nullable=False),
                        sa.Column('is_internal_call', sa.Boolean(), nullable=False),
                        sa.Column('request_date', sa.DateTime(), nullable=False),
                        sa.Column('end_point_id', sa.Integer(), nullable=False),
                        sa.Column('nb', sa.BigInteger(), nullable=True),
                        sa.Column('nb_without_journey', sa.BigInteger(), nullable=True),
                        sa.Column('object_count', sa.BigInteger(), nullable=True),
                        sa.PrimaryKeyConstraint('region_id', 'api', 'user_id', 'app_name', 'is_internal_call',
                                                'request_date', 'end_point_id'),
                        sa.UniqueConstraint('region_id', 'request_date', 'is_internal_call', 'api', 'app_name',
                                            'user_id', 'end_point_id',
                                            name='{schema}_requests_calls_pkey'.format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )
    if not context.dialect.has_table(connection.engine, table_name='token_stats', schema=config.db['schema']):
        op.create_table('token_stats',
                        sa.Column('token', sa.Text(), nullable=False),
                        sa.Column('request_date', sa.DateTime(), nullable=False),
                        sa.Column('nb_req', sa.BigInteger(), nullable=True),
                        sa.PrimaryKeyConstraint('token', 'request_date'),
                        sa.UniqueConstraint('token', 'request_date',
                                            name='{schema}_token_stats_pkey'.format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )
    if not context.dialect.has_table(connection.engine, table_name='users', schema=config.db['schema']):
        op.create_table('users',
                        sa.Column('id', sa.Text(), nullable=False),
                        sa.Column('user_name', sa.Text(), nullable=True),
                        sa.Column('date_first_request', sa.DateTime(), nullable=True),
                        sa.PrimaryKeyConstraint('id'),
                        sa.UniqueConstraint('id', name='{schema}_users_pkey'.format(schema=config.db['schema'])),
                        schema=config.db['schema']
                        )


def downgrade():
    pass
