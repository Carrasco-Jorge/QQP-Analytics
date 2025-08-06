from sqlalchemy import select
from qqp.database.db_models import DBTables as db


def load_processed_files():
    with db.engine.begin() as conn:
        result = conn.execute(
            select(db.processed_files.c.name)
        )
    return list(result.all())


def load_presentations():
    with db.engine.begin() as conn:
        result = conn.execute(
            select(db.product_presentations).where(
                db.product_presentations.c.processed == 1
            )
        )
    return list(result.all())


def load_stores():
    with db.engine.begin() as conn:
        result = conn.execute(
            select(db.dim_stores)
        )
    return list(result.all())


def load_db():
    processed_files = load_processed_files()
    presentations = load_presentations()
    stores = load_stores()

    return processed_files, presentations, stores
