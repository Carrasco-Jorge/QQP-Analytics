from qqp.os_utils import join_paths
from qqp.config import settings


new_stores_path = join_paths([settings.STAGING_CSV_DIR, "new_stores.csv"])
pending_rows_path = join_paths([settings.STAGING_CSV_DIR, "pending_rows.csv"])
pending_products_path = join_paths([settings.STAGING_CSV_DIR, "pending_products.csv"])
# dim_products_path = join_paths([settings.STAGING_CSV_DIR, "dim_product.csv"])
fact_price_path = join_paths([settings.STAGING_CSV_DIR, "fact_price.csv"])
processed_file_path = join_paths([settings.STAGING_CSV_DIR, "file_name.csv"])