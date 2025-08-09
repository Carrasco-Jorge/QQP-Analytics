import datetime
import pandas as pd
from math import ceil
from tqdm import tqdm
from sqlalchemy import insert
from qqp.config import paths
from qqp.database.db_models import DBTables
from qqp.database.sqlite_models import SQLiteTables


tables = [
    {"db": DBTables.fact_price, "sqlite": SQLiteTables.fact_price, "path": paths.fact_price_path},
    {"db": DBTables.products, "sqlite": SQLiteTables.products, "path": paths.pending_products_path},
    {"db": DBTables.pending_rows, "sqlite": SQLiteTables.pending_rows, "path": paths.pending_rows_path},
    {"db": DBTables.stores, "sqlite": SQLiteTables.stores, "path": paths.new_stores_path},
    {"db": DBTables.processed_files, "sqlite": SQLiteTables.processed_files, "path": paths.processed_file_path}
]


def get_records(df: pd.DataFrame) -> list[dict]:
    records = []
    for _, row in df.iterrows():
        record = dict()
        for col, val in row.items():
            if pd.isna(val):
                record[col] = None
            elif isinstance(val, pd.Timestamp):
                record[col] = val.date()
            elif col=="date":
                record[col] = datetime.datetime.strptime(val, "%Y-%m-%d").date()
            else:
                record[col] = val
        records.append(record)
    return records


def insert_csv(batch_size:int=1000):
    for table_dict in tables:
        csv_path = table_dict["path"]
        db_table = table_dict["db"]
        sqlite_table = table_dict["sqlite"]

        csv_df = pd.read_csv(csv_path).replace({float("nan"): None})

        len_csv = csv_df.shape[0]
        num_batches = ceil(len_csv/batch_size)

        with SQLiteTables.engine.begin() as conn:
            for i in tqdm(range(num_batches)):
                start = i * batch_size
                end = min((i+1) * batch_size, len_csv)
                batch = csv_df.iloc[start:end,:]
                records = get_records(batch)
                conn.execute(insert(sqlite_table).values(records))
