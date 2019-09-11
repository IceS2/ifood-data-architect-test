from pyspark.sql import Window
from pyspark.sql.functions import rank, expr, to_date, desc, col


def read_and_polish_orders(orders_path):
    o = spark.read.parquet(orders_path)
    o = o.dropDuplicates(subset=["order_id"]).select([
        "customer_id",
        "delivery_address_city",
        "delivery_address_country",
        "delivery_address_district",
        "delivery_address_external_id",
        "delivery_address_latitude",
        "delivery_address_longitude",
        "delivery_address_state",
        "delivery_address_zip_code",
        "merchant_id",
        "merchant_latitude",
        "merchant_longitude",
        "merchant_timezone",
        "order_created_at",
        "order_id",
        "order_scheduled",
        "order_scheduled_date",
        "order_total_amount",
        "origin_platform"]) \
        .withColumn('order_created_at_merchant_localtime', expr("from_utc_timestamp(order_created_at, merchant_timezone)")) \
        .withColumn('order_created_at_merchant_localtime_date', to_date("order_created_at_merchant_localtime"))
    return o


def read_and_polish_order_statuses(order_statuses_path):
    os = spark.read.parquet(order_statuses_path)
    window = Window.partitionBy("order_id").orderBy(desc("created_at"))
    os = os.withColumn('rank', rank().over(window)) \
        .filter(col('rank') == 1) \
        .drop('rank') \
        .withColumnRenamed('created_at', 'last_status_updated_at') \
        .withColumnRenamed('value', 'status')
    return os


def read_and_polish_consumers(consumers_path):
    c = spark.read.parquet(consumers_path)
    c = c.select([
        "customer_id",
        "language",
        "created_at",
        "active",
        "customer_phone_area"]) \
        .withColumnRenamed("created_at", "customer_created_at") \
        .withColumnRenamed("language", "customer_language") \
        .withColumnRenamed("active", "customer_active")
    return c


def read_and_polish_restaurants(restaurants_path):
    r = spark.read_parquet(restaurants_path)
    r = r.withColumnRenamed("id", "merchant_id") \
        .withColumnRenamed("created_at", "merchant_created_at") \
        .withColumnRenamed("enabled", "merchant_enabled") \
        .withColumnRenamed("price_range", "merchant_price_range") \
        .withColumnRenamed("average_ticket", "merchang_average_ticket") \
        .withColumnRenamed("takeout_time", "merchant_takeout_time") \
        .withColumnRenamed("delivery_time", "merchant_delivery_time") \
        .withColumnRenamed("minimum_order_value", "merchant_minimum_order_value")
    return r


def create_orders_datamart(orders_path, order_statuses_path, consumers_path, restaurants_path, orders_datamart_path):
    orders = read_and_polish_orders(orders_path)
    order_statuses = read_and_polish_order_statuses(order_statuses_path)
    restaurants = read_and_polish_restaurants(restaurants_path)
    consumers = read_and_polish_consumers(consumers_path)

    orders = orders.join(restaurants, 'merchant_id', 'left').join(consumers, 'customer_id', 'left').join(order_statuses, 'order_id', 'left')
    orders.repartition('order_created_at_merchant_localtime_date') \
        .write.parquet(orders_datamart_path, mode='overwrite', compression='snappy', partitionBy='order_created_at_merchant_localtime_date')
