from pyspark.sql.functions import first


def create_order_statuses_datamart(order_statuses_path, order_statuses_datamart_path):
    order_statuses = spark.read.parquet(order_statuses_path)
    order_statuses = order_statuses.groupby("order_id").pivot("value").agg(first("created_at"))
    
    order_statuses.repartition(10) \
        .write.parquet(order_statuses_datamart_path, mode='overwrite', compression='snappy')