from pyspark.sql.functions import explode, from_json
from pyspark.sql.types import StructType, ArrayType



def infer_schema(order_items):
    return spark.read.json(order_items.rdd.map(lambda row: row.items)).schema


def fix_schema(schema):
    fixed_schema = StructType()

    for field in schema.fields:
        if field.name != "_corrupt_record":
            fixed_schema.add(field)

    fixed_schema = ArrayType(fixed_schema)
    return fixed_schema

def create_order_items_datamart(orders_path, order_items_datamart_path):
    order_items = spark.read.parquet(orders_path).select(['items', 'order_id'])

    schema = infer_schema(order_items)
    schema = fix_schema(schema)

    order_items = order_items.select(from_json("items", schema).alias("items"), "order_id") \
        .select(explode("items").alias("item"), "order_id") \
        .select("item.*", "order_id") \
        .dropDuplicates()
    
    order_items.repartition(10) \
        .write.parquet(order_items_datamart_path, mode='overwrite', compression='snappy')
