import sqlalchemy
from pandas import Series
from sqlalchemy.ext.declarative import declarative_base
from migrasi.source1.mst_kabupaten import mst_kabupaten

from source.migrate import migrate as common_migrate

Base = declarative_base()


class mst_kecamatan(Base):
    __tablename__ = 'mst_kecamatan'
    id = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    kode_kabupaten = sqlalchemy.Column(sqlalchemy.String(length=255), sqlalchemy.ForeignKey(mst_kabupaten.kode_kabupaten))
    kode_kecamatan = sqlalchemy.Column(sqlalchemy.String(length=255), primary_key=True)
    nama_kecamatan = sqlalchemy.Column(sqlalchemy.String(length=255), nullable=False)


def sync_entity(new_entity: mst_kecamatan, old_entity: Series, _):
    new_entity.id = old_entity['id_kecamatan']
    new_entity.kode_kabupaten = old_entity['kode_kabupaten']
    new_entity.kode_kecamatan = old_entity['kode_kecamatan']
    new_entity.nama_kecamatan = old_entity['nama_kecamatan']

    return new_entity


def migrate(source, db_connection_sink):
    print("Migrating mst_kecamatan....")

    common_migrate(
        old_table_name=mst_kecamatan.__tablename__,
        table_name=mst_kecamatan.__tablename__,
        db_connection_sink=db_connection_sink,
        source='source1',
        table_model=mst_kecamatan,
        sync_entity=sync_entity,
        old_table_identifier="id_kecamatan"
    )
