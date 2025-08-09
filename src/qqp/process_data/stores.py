import pandas as pd
from qqp.config import paths
from qqp.config.settings import STAGING_CSV_DIR, default_encoding
from qqp.process_data.settings import store_columns

def get_unique_stores(df_puebla: pd.DataFrame):
    stores = df_puebla[store_columns].copy()
    unique_stores = stores.drop_duplicates()

    return unique_stores


def get_new_stores(unique_stores: pd.DataFrame, existing_stores: pd.DataFrame) -> pd.DataFrame:
    max_id = existing_stores["id"].max()
    last_id = 0 if pd.isna(max_id) else max_id
    existing_stores = existing_stores.drop(columns=["id", "lat", "long"])

    new_stores = unique_stores.merge(
        existing_stores.drop_duplicates(),
        how="left",
        on=store_columns[:-2],
        indicator=True
    )
    new_stores = new_stores[new_stores["_merge"] == "left_only"].drop(columns=["_merge"])
    
    new_stores["id"] = [last_id+i+1 for i in range(new_stores.shape[0])]
    new_stores = new_stores[["id"] + store_columns]

    return new_stores


def save_new_stores_csv(new_stores: pd.DataFrame):
    path = paths.new_stores_path
    new_stores.to_csv(
        path,
        index=False,
        encoding=default_encoding
    )

    return None

def process_stores(df_puebla: pd.DataFrame, existing_stores: pd.DataFrame) -> pd.DataFrame:
    unique_stores = get_unique_stores(df_puebla)
    new_stores = get_new_stores(unique_stores, existing_stores)
    save_new_stores_csv(new_stores)

    updated_stores = pd.concat([existing_stores, new_stores])
    return updated_stores