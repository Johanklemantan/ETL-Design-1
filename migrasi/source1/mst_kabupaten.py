import sqlalchemy
from pandas import Series
from sqlalchemy.ext.declarative import declarative_base
from migrasi.source2.mst_provinsi import mst_provinsi

from source.migrate import migrate as common_migrate

Base = declarative_base()


class mst_kabupaten(Base):
    __tablename__ = 'mst_kabupaten'
    id = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    kode_provinsi = sqlalchemy.Column(sqlalchemy.String(length=255), sqlalchemy.ForeignKey(mst_provinsi.kode_provinsi))
    kode_kabupaten = sqlalchemy.Column(sqlalchemy.String(length=255), primary_key=True)
    nama_kabupaten = sqlalchemy.Column(sqlalchemy.String(length=255), nullable=False)


def sync_entity(new_entity: mst_kabupaten, old_entity: Series, _):
    new_entity.id = old_entity['id_kabupaten']
    new_entity.kode_provinsi = old_entity['kode_provinsi']
    new_entity.kode_kabupaten = old_entity['kode_kabupaten']
    new_entity.nama_kabupaten = old_entity['nama_kabupaten']

    return new_entity


def migrate(source, db_connection_sink):
    print("Migrating mst_kabupaten....")

    common_migrate(
        old_table_name=mst_kabupaten.__tablename__,
        table_name=mst_kabupaten.__tablename__,
        db_connection_sink=db_connection_sink,
        source='source1',
        table_model=mst_kabupaten,
        sync_entity=sync_entity,
        old_table_identifier="id_kabupaten"
    )
