from qqp.database.sqlite_models import SQLiteTables as dblite

def reset_sqlite_file():
    with dblite.engine.begin() as conn:
        for table_name, table in dblite.metadata.tables.items():
            print(table_name)
            conn.execute(table.delete())
