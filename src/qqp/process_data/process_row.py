import pandas as pd
import csv
from tqdm import tqdm
from qqp.config.settings import csv_columns
from qqp.process_data.settings import product_columns, store_key_cols
from qqp.config import paths
from qqp.database.db_models import DBTables


full_product_columns = [col.name for col in DBTables.products.columns]
# dim_products_columns = [col.name for col in DBTables.dim_products.columns]
fact_price_columns = [col.name for col in DBTables.fact_price.columns]


def init_csv_files():
    with open(paths.pending_rows_path, "w") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"')
        writer.writerow(csv_columns)
    
    with open(paths.pending_products_path, "w") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"')
        writer.writerow(full_product_columns)
    
    # with open(paths.dim_products_path, "w") as file:
    #     writer = csv.writer(file, delimiter=',', quotechar='"')
    #     writer.writerow(products_columns)

    with open(paths.fact_price_path, "w") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"')
        writer.writerow(fact_price_columns)

    return None

def get_presentation_state(
        row: pd.Series,
        products: dict
    ) -> tuple[bool, bool, dict | None]:
    product_key_values = row[product_columns]
    product_key = "_".join(product_key_values.astype(str))
    product_info = products["dict"].get(product_key)
    
    if product_info is None:
        return False, False, None
    
    already_processed = True if product_info["processed"] == 1 else False

    return True, already_processed, product_info


def extract_row_info(row: pd.Series, product_info: dict | None, stores: pd.DataFrame):
    if product_info is None:
        raise Exception(f"'product_info' must not be None. {product_info}")
    
    date = row["date"].date()
    product_id = product_info["id"]

    store_filters = {col: val for col, val in zip(store_key_cols, row[store_key_cols])}
    store_id = (stores[
        (stores[list(store_filters)]==pd.Series(store_filters)).all(axis=1)
    ]).iloc[0]["id"]

    price = row["price"]
    std_qty = product_info["std_quantity"]

    std_price = round(price / std_qty, 2)

    return date, product_id, store_id, std_price


def save_price_row(price_row):
    with open(paths.fact_price_path, "a") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"')
        writer.writerow(price_row)


def save_pending_row(row: pd.Series):
    pending_row = row.copy()
    pending_row["date"] = pending_row["date"].date()
    pending_row = pending_row.reindex(csv_columns).to_list()
    with open(paths.pending_rows_path, "a") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"')
        writer.writerow(pending_row)


def save_pending_product(
        row: pd.Series,
        products: dict
    ):
    product_id = products["max_id"] + 1

    pending_presentation = [
        product_id, row["category"], row["type"], row["product"],
        row["presentation"], row["brand"], 0, "", "", "", "", ""
    ]

    # SAVE IN CSV
    with open(paths.pending_products_path, "a") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"')
        writer.writerow(pending_presentation)
    
    # UPDATE DICT
    new_key = "_".join(pending_presentation[1:6])
    products["dict"][new_key] = {
        "id": product_id,
        "processed": 0,
        "variant": None,
        "unit": None,
        "quantity": None,
        "std_unit": None,
        "std_quantity": None
    }
    products["max_id"] += 1


def process_row(
        row: pd.Series,
        products: dict,
        stores: pd.DataFrame
) -> int:
    in_db, already_processed, product_info = get_presentation_state(row, products)

    if already_processed:
        price_row = extract_row_info(row, product_info, stores)
        save_price_row(price_row)
        return 1

    if not in_db:
        save_pending_product(
            row,
            products
        )
    save_pending_row(row)
    return 0


def process_rows(
        df_puebla: pd.DataFrame,
        products: dict,
        stores: pd.DataFrame
) -> tuple[int, int]:
    init_csv_files()

    num_rows = df_puebla.shape[0]
    saved_rows = 0
    for i in tqdm(range(num_rows)):
        row = df_puebla.iloc[i]
        saved_rows += process_row(
            row,
            products,
            stores
        )

        # max_rows = 3
        # if i == max_rows - 1:
        #     break
        
    pending_rows = num_rows - saved_rows

    return saved_rows, pending_rows
