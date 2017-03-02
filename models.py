from sqlalchemy import Column, Integer, Text, Date, BIGINT, UniqueConstraint, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData

Base = declarative_base(metadata=MetaData(schema='stat_compiled'))


class CoverageStartEndNetwork(Base):
    __tablename__ = 'coverage_start_end_networks'

    region_id = Column(Text(), primary_key=True,  nullable=False)
    start_network_id = Column(Text(), primary_key=True,  nullable=False)
    start_network_name = Column(Text(), primary_key=False,  nullable=False)
    end_network_id = Column(Text(), primary_key=True,  nullable=False)
    end_network_name = Column(Text(), primary_key=False,  nullable=False)
    request_date = Column(Date(), primary_key=True,  nullable=False)
    is_internal_call = Column(Integer(), primary_key=True,  nullable=False)
    nb = Column(BIGINT(), primary_key=False,  nullable=False)
    __table_args__ = (
        UniqueConstraint(
            'region_id', 'start_network_id', 'end_network_id', 'request_date', 'is_internal_call',
            name='coverage_start_end_networks_pkey'
        ),
    )


class Users(Base):
    __tablename__ = 'users'

    id = Column(Text(), primary_key=True,  nullable=False)
    user_name = Column(Text(), primary_key=False,  nullable=True)
    date_first_request = Column(DateTime(), primary_key=False,  nullable=True)
    __table_args__ = (
        UniqueConstraint(
            'id', name='users_pkey'
        ),
    )
