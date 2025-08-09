from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = str(os.getenv("DATABASE_URL"))
SQLITE_URL = str(os.getenv("SQLITE_URL"))
RAW_DATA_DIR = str(os.getenv("RAW_DATA_DIR"))
STAGING_CSV_DIR = str(os.getenv("STAGING_CSV_DIR"))
DB_DATA_DIR = str(os.getenv("DB_DATA_DIR"))

engine_echo=False

default_encoding = "utf-8"
alternative_encoding = "cp850"

csv_columns=[
    "product", "presentation", "brand", "type", "category",
    "price", "date", "chain", "line_of_business", "branch",
    "address", "state", "municipality", "lat", "long"
]
