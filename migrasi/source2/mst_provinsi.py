import sqlalchemy
from pandas import Series
from sqlalchemy.ext.declarative import declarative_base

from source.migrate import migrate as common_migrate

Base = declarative_base()


class mst_provinsi(Base):
    __tablename__ = 'mst_provinsi'
    id = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    kode_provinsi = sqlalchemy.Column(sqlalchemy.String(length=255), primary_key=True)
    nama_provinsi = sqlalchemy.Column(sqlalchemy.String(length=255), nullable=False)


def sync_entity(new_entity: mst_provinsi, old_entity: Series, _):
    new_entity.id = old_entity['id_provinsi']
    new_entity.kode_provinsi = old_entity['kode_provinsi']
    new_entity.nama_provinsi = old_entity['nama_provinsi']

    return new_entity


def migrate(source, db_connection_sink):
    print("Migrating mst_provinsi....")

    common_migrate(
        old_table_name=mst_provinsi.__tablename__,
        table_name=mst_provinsi.__tablename__,
        db_connection_sink=db_connection_sink,
        source='source2',
        table_model=mst_provinsi,
        sync_entity=sync_entity,
        old_table_identifier="id_provinsi"
    )
