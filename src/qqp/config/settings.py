from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = str(os.getenv("DATABASE_URL"))
SQLITE_URL = str(os.getenv("SQLITE_URL"))
