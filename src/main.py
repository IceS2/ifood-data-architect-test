import pyspark
import os
import sys

if os.path.exists('raw_layer.zip'):
    sys.path.insert(0, 'raw_layer.zip')
else:
    sys.path.insert(0, './raw_layer')
    
if os.path.exists('datamarts.zip'):
    sys.path.insert(0, 'datamarts.zip')
else:
    sys.path.insert(0, './datamarts')

from raw_layer.raw_data import save_raw_data_as_parquet
from datamarts.orders import create_orders_datamart
from datamarts.order_items import create_order_items_datamart
from datamarts.order_statuses import create_order_statuses_datamart

ORDERS_PARQUET_PATH = "orders"
ORDER_STATUSES_PARQUET_PATH = "order_statuses"
RESTAURANTS_PARQUET_PATH = "restaurants"
CONSUMERS_PARQUET_PATH = "consumers"

ORDERS_DATAMART_PATH = "orders_datamart"
ORDER_ITEMS_DATAMART_PATH = "order_items_datamart"
ORDER_STATUSES_DATAMART_PATH = "order_statuses_datamart_path"

RAW_DATA_SOURCES = [
    {
        "file_path": "s3://ifood-data-architect-test-source/order.json.gz",
        "parquet_file_path": ORDERS_PARQUET_PATH,
        "data_type": "json",
        "id_column": "order_id",
        "number_of_partitions": 10
    },
    {
        "file_path": "s3://ifood-data-architect-test-source/status.json.gz",
        "parquet_file_path": ORDER_STATUSES_PARQUET_PATH,
        "data_type": "json",
        "id_column": "status_id",
        "number_of_partitions": 2
    },
    {
        "file_path": "s3://ifood-data-architect-test-source/restaurant.csv.gz",
        "parquet_file_path": RESTAURANTS_PARQUET_PATH,
        "data_type": "csv",
        "id_column": "id",
        "number_of_partitions": 1,
        "kwargs": {"header": True, "schema": "id STRING, created_at TIMESTAMP, enabled BOOLEAN, price_range INTEGER, average_ticket FLOAT, takeout_time integer, delivery_time integer, minimum_order_value FLOAT, merchant_zip_code STRING, merchant_city STRING, merchant_state STRING, merchant_country STRING"}
    },
    {
        "file_path": "s3://ifood-data-architect-test-source/consumer.csv.gz",
        "parquet_file_path": CONSUMERS_PARQUET_PATH,
        "data_type": "csv",
        "id_column": "customer_id",
        "number_of_partitions": 1,
        "kwargs": {"header": True, "schema": "customer_id STRING, language STRING, created_at TIMESTAMP, active BOOLEAN, customer_name STRING, customer_phone_area STRING, customer_phone_number STRING"}
    },
]


if __name__ == "__main__":
    sc = pyspark.SparkContext(appName='ifood-data-architect-test')

    save_raw_data_as_parquet(RAW_DATA_SOURCES)
    create_orders_datamart(ORDERS_PARQUET_PATH, ORDER_STATUSES_PARQUET_PATH, CONSUMERS_PARQUET_PATH, RESTAURANTS_PARQUET_PATH, ORDERS_DATAMART_PATH)
    create_order_items_datamart(ORDERS_PARQUET_PATH, ORDER_ITEMS_DATAMART_PATH)
    create_order_statuses_datamart(ORDER_STATUSES_PARQUET_PATH, ORDER_STATUSES_DATAMART_PATH)

    sc.close()