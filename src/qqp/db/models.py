from sqlalchemy import MetaData, Table, Column, Integer, String

metadata_obj = MetaData()

units = Table(
    "unidades",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("nombre", String(45), nullable=False),
    Column("unidad", String(45), nullable=False),
    Column("mide", String(45), nullable=False),
    Column("unidad_std", String(45), nullable=False),
    Column("cantidad_std", Integer, nullable=False)
)

processed_files = Table(
    "archivos_procesados",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("nombre", String(45), nullable=False)
)

product_presentations = Table(
    "presentaciones_productos",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("producto", String(100), nullable=False),
    Column("presentacion", String(255), nullable=False),
    Column("procesado", Integer, nullable=False),
    Column("tipo_producto", String(45)),
    Column("unidad", String(45)),
    Column("nombre", Integer),
)

def create_tables(engine):
    metadata_obj.create_all(engine)
