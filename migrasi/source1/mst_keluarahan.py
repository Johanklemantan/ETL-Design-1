import sqlalchemy
from pandas import Series
from sqlalchemy.ext.declarative import declarative_base
from migrasi.source1.mst_kecamatan import mst_kecamatan

from source.migrate import migrate as common_migrate

Base = declarative_base()


class mst_kelurahan(Base):
    __tablename__ = 'mst_kelurahan'
    id = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    kode_kecamatan = sqlalchemy.Column(sqlalchemy.String(length=255), sqlalchemy.ForeignKey(mst_kecamatan.kode_kecamatan))
    kode_kelurahan = sqlalchemy.Column(sqlalchemy.String(length=255), primary_key=True)
    nama_kelurahan = sqlalchemy.Column(sqlalchemy.String(length=255), nullable=False)


def sync_entity(new_entity: mst_kelurahan, old_entity: Series, _):
    new_entity.id = old_entity['id_kelurahan']
    new_entity.kode_kecamatan = old_entity['kode_kecamatan']
    new_entity.kode_kelurahan = old_entity['kode_kelurahan']
    new_entity.nama_kelurahan = old_entity['nama_kelurahan']

    return new_entity


def migrate(source, db_connection_sink):
    print("Migrating mst_kelurahan....")

    common_migrate(
        old_table_name=mst_kelurahan.__tablename__,
        table_name=mst_kelurahan.__tablename__,
        db_connection_sink=db_connection_sink,
        source='source1',
        table_model=mst_kelurahan,
        sync_entity=sync_entity,
        old_table_identifier="id_kelurahan"
    )
