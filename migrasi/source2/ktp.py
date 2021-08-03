import sqlalchemy
from pandas import Series
from sqlalchemy.ext.declarative import declarative_base

from source.migrate import migrate as common_migrate

Base = declarative_base()


class ktp(Base):
    __tablename__ = 'ktp'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    no_ktp = sqlalchemy.Column(sqlalchemy.String(length=255), nullable=False)


def sync_entity(new_entity: ktp, old_entity: Series, _):
    new_entity.id_ = old_entity['id_ktp']
    new_entity.no_ktp = old_entity['no_ktp']

    return new_entity


def migrate(source, db_connection_sink):
    print("Migrating ktp....")

    common_migrate(
        old_table_name=ktp.__tablename__,
        table_name=ktp.__tablename__,
        db_connection_sink=db_connection_sink,
        source='source2',
        table_model=ktp,
        sync_entity=sync_entity,
        old_table_identifier="id_ktp"
    )
