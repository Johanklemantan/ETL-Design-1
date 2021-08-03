from source.database import *

def transform():
    query = """select 
    a.id,
    a.no_ktp,
    mp.nama_provinsi,
    mk.nama_kabupaten,
    case 
    when MID(a.no_ktp,7,2) > 40 then 'perempuan' else 'laki - laki'
    end as jenis_kelamin,
    CONCAT(a.tanggal_lahir,'-',a.bulan_lahir,'-',a.tahun_lahir) as tanggal_kelahiran
    from(
    SELECT *,
    LEFT(no_ktp,2) as kode_provinsi,
    LEFT(no_ktp,4) as kode_kabupaten,
    case
    when MID(no_ktp,7,2) > 40 then MID(no_ktp,7,2)-40 else MID(no_ktp,7,2) 
    end as tanggal_lahir,
    MID(no_ktp,9,2) as bulan_lahir,
    case 
    when MID(no_ktp,11,2) > 20 then CONCAT('19',MID(no_ktp,11,2)) else CONCAT('20',MID(no_ktp,11,2)) 
    end as tahun_lahir
    FROM ktp) a
    join mst_provinsi mp 
    on a.kode_provinsi = mp.kode_provinsi
    join mst_kabupaten mk 
    on a.kode_kabupaten = mk.kode_kabupaten ;"""
    print('Start Transforming..')
    db_connection_sink = get_db_connection_sink()
    df_result = get(query,'sink')
    df_result.to_sql(name='tabel_hasil',con=db_connection_sink,if_exists='append',index=False)
    print('Transformation Success.')

if __name__ == "__transform__":
    transform()