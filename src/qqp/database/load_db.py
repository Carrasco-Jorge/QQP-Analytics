import pandas as pd
from sqlalchemy import select
from sqlalchemy.sql.expression import func
from qqp.database.db_models import DBTables
from qqp.database.sqlite_models import SQLiteTables
from qqp.process_data.settings import store_columns


def load_processed_files(models: type[DBTables] | type[SQLiteTables]) -> dict:
    with models.engine.begin() as conn:
        result = conn.execute(
            select(models.processed_files)
        )

        files_dict = {"files": [], "max_id": 0}
        for file_id, file_name in result:
            file_name = str(file_name)
            file_id = int(file_id)

            files_dict["files"].append(file_name)

            if files_dict["max_id"] < file_id:
                files_dict["max_id"] = file_id
    return files_dict

def get_processed_files() -> dict:
    db_files_dict = load_processed_files(DBTables)
    sqlite_files_dict = load_processed_files(SQLiteTables)

    files_dict = {
        "files": db_files_dict["files"] + sqlite_files_dict["files"],
        "max_id": max(db_files_dict["max_id"], sqlite_files_dict["max_id"])
    }
    return files_dict



def load_products(models: type[DBTables] | type[SQLiteTables]) -> dict:
    with models.engine.begin() as conn:
        result = conn.execute(
            select(models.products)
        )

        products_dict = {"dict": dict(), "max_id": 0}
        for row in result:
            product_id = int(row[0])
            product_key = "_".join([row[i] for i in [1,2,3,4,5]])
            products_dict["dict"][product_key] = {
                "id": product_id,
                "processed":int(row[6]),
                "variant": str(row[7]) if row[7] is not None else None,
                "unit": str(row[8]) if row[8] is not None else None,
                "quantity": float(row[9]) if row[9] is not None else None,
                "std_unit": str(row[10]) if row[10] is not None else None,
                "std_quantity": float(row[11]) if row[11] is not None else None
            }
            
            if products_dict["max_id"] < product_id:
                products_dict["max_id"] = product_id
    return products_dict

def get_products():
    db_products = load_products(DBTables)
    sqlite_products = load_products(SQLiteTables)
    
    products = {
        "dict": db_products["dict"] | sqlite_products["dict"],
        "max_id": max(db_products["max_id"], sqlite_products["max_id"])
    }
    return products



def load_stores(models: type[DBTables] | type[SQLiteTables]) -> pd.DataFrame:
    with models.engine.begin() as conn:
        result = conn.execute(
            select(models.stores)
        )
        stores = pd.DataFrame(data=list(result.all()), columns=["id"]+store_columns)
        stores = stores.astype({
            "id": int,
            "lat": float,
            "long": float
        })
    return stores

def get_stores():
    db_stores = load_stores(DBTables)
    sqlite_stores = load_stores(SQLiteTables)
    stores = [db_stores, sqlite_stores]
    return pd.concat([df for df in stores if not df.empty], ignore_index=True)



def load_current_and_pending():
    files_dict = get_processed_files()
    products = get_products()
    stores = get_stores()
    return files_dict, products, stores


def load_database():
    DBTables.create_tables()
    SQLiteTables.create_tables()

    try:
        files_dict, products, stores = load_current_and_pending()
    except Exception as e:
        print(f"Could not load database info. {e}")
        exit()

    # print("Processed files:", files_dict)
    # print("Products:", products)
    # print("Stores:", stores)

    return files_dict, products, stores


if __name__=="__main__":
    load_database()