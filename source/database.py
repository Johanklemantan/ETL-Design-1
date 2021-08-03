import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

sink = json.load(open('C:/Work/ETL/ETL-1/config/data-sink.json'))
source1 = json.load(open('C:/Work/ETL/ETL-1/config/data-source1.json'))
source2 = json.load(open('C:/Work/ETL/ETL-1/config/data-source2.json'))


def get_connection_string(json_file):
    data = json_file

    return (
            data.get("driver") + data.get("username") + ":" + data.get("password") + "@" +
            data.get("host") + ":" + data.get("port") + "/" + data.get("database")
    )


def get_db_connection_source1():
    db_connection_source1 = create_engine(get_connection_string(source1))
    return db_connection_source1


def get_db_connection_source2():
    db_connection_source2 = create_engine(get_connection_string(source2))
    return db_connection_source2
    

def get_db_connection_sink():
    db_connection_sink = create_engine(get_connection_string(sink))
    return db_connection_sink
 

def get_df(table_name, target_db):
    if target_db == 'sink':
        db_connection = get_db_connection_sink()
    elif target_db == 'source1':
        db_connection = get_db_connection_source1()
    elif target_db == 'source2':
        db_connection = get_db_connection_source2()

    df = pd.read_sql('SELECT * FROM ' + table_name, db_connection)
    return df


def drop_all(target_db):
    if target_db == 'sink':
        db_connection = get_db_connection_sink()
    elif target_db == 'source1':
        db_connection = get_db_connection_source1()
    elif target_db == 'source2':
        db_connection = get_db_connection_source2()

    db_name = str(db_connection.url.database)
    execute(f"DROP DATABASE {db_name}", db_connection)
    execute(f"CREATE DATABASE {db_name}", db_connection)

    return


def get(query, target_db):
    if target_db == 'sink':
        db_connection = get_db_connection_sink()
    elif target_db == 'source1':
        db_connection = get_db_connection_source1()
    elif target_db == 'source2':
        db_connection = get_db_connection_source2()

    df = pd.read_sql(query, db_connection)
    return df


def execute(query, db_connection):
    connection = db_connection.connect()
    connection.execute(query)
    connection.close()


def get_count(table_name, target_db):
    if target_db == 'sink':
        db_connection = get_db_connection_sink()
    elif target_db == 'source1':
        db_connection = get_db_connection_source1()
    elif target_db == 'source2':
        db_connection = get_db_connection_source2()

    df = pd.read_sql('SELECT COUNT(1) FROM ' + table_name, db_connection)
    return df