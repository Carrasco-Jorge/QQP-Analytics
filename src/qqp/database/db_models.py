from sqlalchemy import MetaData, Table, Column, Integer, String, Float, Date
from qqp.database.engines import db_engine

class DBTables:
    engine = db_engine
    metadata = MetaData()

    units = Table(
        "units",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(45), nullable=False),
        Column("unit", String(45), nullable=False),
        Column("measures", String(45), nullable=False),
        Column("std_unit", String(45), nullable=False),
        Column("std_quantity", Integer, nullable=False)
    )

    processed_files = Table(
        "processed_files",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(45), nullable=False)
    )

    products = Table(
        "products",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("category", String(45), nullable=False),
        Column("type", String(45), nullable=False),
        Column("product", String(100), nullable=False),
        Column("presentation", String(200), nullable=False),
        Column("brand", String(50), nullable=False),
        Column("processed", Integer, nullable=False),
        Column("variant", String(45)),
        Column("unit", String(45)),
        Column("quantity", Integer),
        Column("std_unit", String(45)),
        Column("std_quantity", Float)
    )

    pending_rows = Table(
        "pending_rows",
        metadata,
        Column("product", String(100), nullable=False),
        Column("presentation", String(200), nullable=False),
        Column("brand", String(50), nullable=False),
        Column("type", String(45), nullable=False),
        Column("category", String(45), nullable=False),
        Column("price", Float, nullable=False),
        Column("date", Date, nullable=False),
        Column("chain", String(45), nullable=False),
        Column("line_of_business", String(50), nullable=False),
        Column("branch", String(100), nullable=False),
        Column("address", String(300), nullable=False),
        Column("state", String(45), nullable=False),
        Column("municipality", String(45), nullable=False),
        Column("lat", Float, nullable=False),
        Column("long", Float, nullable=False)
    )

    # dim_products = Table(
    #     "dim_products",
    #     metadata,
    #     Column("id", Integer, primary_key=True),
    #     Column("category", String(45), nullable=False),
    #     Column("type", String(45), nullable=False),
    #     Column("product", String(100), nullable=False),
    #     Column("variant", String(45), nullable=False),
    #     Column("brand", String(50), nullable=False),
    #     Column("std_unit", String(45), nullable=False),
    #     Column("std_quantity", Float, nullable=False)
    # )

    stores = Table(
        "stores",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("line_of_business", String(50), nullable=False),
        Column("chain", String(45), nullable=False),
        Column("branch", String(100), nullable=False),
        Column("address", String(300), nullable=False),
        Column("lat", Float, nullable=False),
        Column("long", Float, nullable=False)
    )

    fact_price = Table(
        "fact_price",
        metadata,
        Column("date", Date, nullable=False),
        Column("product_id", Integer, nullable=False),
        Column("store_id", Integer, nullable=False),
        Column("std_price", Float, nullable=False)
    )

    @classmethod
    def create_tables(cls):
        cls.metadata.create_all(cls.engine)
