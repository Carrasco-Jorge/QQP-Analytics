import pandas as pd
from qqp.config.settings import store_columns, STAGING_CSV_DIR, default_encoding
from qqp.io_utils import join_paths

def get_unique_stores(df_puebla: pd.DataFrame):
    stores = df_puebla[store_columns].copy()
    unique_stores = stores.drop_duplicates()

    return unique_stores


def get_new_stores(unique_stores: pd.DataFrame, existing_stores: list) -> pd.DataFrame:
    stores_df = pd.DataFrame(
        data=existing_stores,
        columns=["id"] + store_columns
    )

    max_id = stores_df["id"].max()
    last_id = 0 if pd.isna(max_id) else max_id
    stores_df = stores_df.drop(columns=["id"])

    new_stores = unique_stores.merge(stores_df.drop_duplicates(), how="left", indicator=True)
    new_stores = new_stores[new_stores["_merge"] == "left_only"].drop(columns=["_merge"])
    
    new_stores["id"] = [i+1 for i in range(last_id, new_stores.shape[0], 1)]
    new_stores = new_stores[["id"] + store_columns]

    return new_stores


def save_new_stores_csv(new_stores: pd.DataFrame):
    path = join_paths([STAGING_CSV_DIR, "unique_stores.csv"])
    new_stores.to_csv(
        path,
        index=False,
        encoding=default_encoding
    )

    return None

def process_stores(df_puebla: pd.DataFrame, existing_stores: list) -> None:
    unique_stores = get_unique_stores(df_puebla)
    stores_df = get_new_stores(unique_stores, existing_stores)
    save_new_stores_csv(stores_df)

    return None