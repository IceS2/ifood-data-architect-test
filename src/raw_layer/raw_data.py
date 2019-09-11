from pyspark.sql.functions import countDistinct


def read_raw_data(file_path, data_type, *args, **kwargs):
    if data_type == "json":
        return spark.read.json(file_path, **kwargs)
    elif data_type == "csv":
        return spark.read.csv(file_path, **kwargs)
    else:
        raise ValueError(
            "{} data_type is not yet implemented.".format(data_type))


def deduplicate_records(data):
    return data.dropDuplicates()


def save_as_parquet(data, parquet_file_path, number_of_partitions):
    data.repartition(number_of_partitions) \
        .write.parquet("{}.parquet".format(parquet_file_path), mode=mode, compression=compression)


def validate_id_count(data, parquet_file_path, id_column):
    parquet_data = spark.read.parquet(parquet_file_path).select(id_column)
    return data.select(countDistinct(id_column)).first() == parquet_data.select(countDistinct(id_column)).first()


def save_raw_data_as_parquet(raw_data_sources):
    for raw_data in raw_data_sources:
        data = read_raw_data(raw_data.get('file_path'), raw_data.get('data_type'), raw_data.get('kwargs'))
        save_as_parquet(deduplicate_records(data), raw_data.get('parquet_file_path'), raw_data.get('number_of_partitions'))
        if not validate_id_count(data, raw_data.get('parquet_file_path'), raw_data.get('id_column')):
            raise Exception

