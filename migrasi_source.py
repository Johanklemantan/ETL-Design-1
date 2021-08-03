from source.database import *
from migrasi.source2.ktp import migrate as migrate_ktp
from migrasi.source2.mst_provinsi import migrate as migrate_mst_provinsi
from migrasi.source1.mst_kabupaten import migrate as migrate_mst_kabupaten
from migrasi.source1.mst_kecamatan import migrate as migrate_mst_kecamatan
from migrasi.source1.mst_keluarahan import migrate as migrate_mst_kelurahan

def main():
    print("Starting Migrate data")
    db_connection_source1 = get_db_connection_source1()
    db_connection_source2 = get_db_connection_source2()
    db_connection_sink = get_db_connection_sink()


    drop_all('sink')

    migrate_ktp(db_connection_source2, db_connection_sink)
    migrate_mst_provinsi(db_connection_source2, db_connection_sink)
    migrate_mst_kabupaten(db_connection_source1, db_connection_sink)
    # migrate_mst_kecamatan(db_connection_source1, db_connection_sink)
    # migrate_mst_kelurahan(db_connection_source1, db_connection_sink)


if __name__ == "__main__":
    main()
