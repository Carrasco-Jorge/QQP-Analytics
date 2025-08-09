from sqlalchemy import create_engine
from qqp.config import settings

db_engine = create_engine(settings.DATABASE_URL, echo=settings.engine_echo)

sqlite_engine = create_engine(settings.SQLITE_URL, echo=settings.engine_echo)