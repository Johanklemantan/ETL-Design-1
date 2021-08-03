from collections.abc import Callable
from datetime import datetime

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

from source.database import get_df, get_count

def migrate(
        old_table_name: str,
        table_name: str,
        source:str,
        db_connection_sink,
        sync_entity: Callable,
        table_model,
        old_table_identifier: str,
        debug: bool = False
):
    Base = declarative_base()

    table_model.__table__.create(bind=db_connection_sink, checkfirst=True)
    Base.metadata.create_all(db_connection_sink)

    # if not migrate_data:
    #     return

    df = get_df(old_table_name, source)
    old_count = get_count(old_table_name, source).values[0][0]

    session_maker = sqlalchemy.orm.sessionmaker()
    session_maker.configure(bind=db_connection_sink)
    session = session_maker()

    for index, row in df.iterrows():
        if debug:
            print(row)

        existing_data = session.query(table_model).filter(
            table_model.id == row[old_table_identifier]
        ).one_or_none()

        if existing_data:
            sync_entity(existing_data, row, db_connection_sink)
        else:
            new_data = table_model()
            sync_entity(new_data, row, db_connection_sink)
            session.add(new_data)

        session.commit()

    new_count = session.query(table_model).count()
    print(table_name, ": old_count: ", old_count, ", new_count: ", new_count)

    session.close()

