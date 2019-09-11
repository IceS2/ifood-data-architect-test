# iFood Data Architect Test Solution

## Challenge

In order to view the challenge, please refer to the [Challenge description](test_challenge.md)


## Raw Layer

My idea is to use AWS S3 as our storage. I'm more familiar with AWS S3, although the paths within the code are as if we were storing the data locally in the spark cluster due to testing the code using databricks without having a S3 bucket to use.

Using AWS S3 we're free to use other tools for our SQL layer, such as Redshift Spectrum, AWS Athena or even Snowflake or Hive if well configured. I'm using parquet as the file type because I'm able to preserve the schema and the read performance is great.

I've noticed two issues with our data:

* We've got some duplicated data within 'order_statuses'. This is not a big deal as a simple dropDuplicates() will work in order to deduplicate the data.
* We've got some duplicated 'order_ids' within orders. Although the 'order_ids' are duplicated, the customer 'cpf' differs. I tried to check inside the customer data if we had the customer 'cpf' in order to curate and save the right one but we don't have that information. I decided to store duplicated 'order_ids' in our raw_layer so we don't lose data and we're able to further investigate the issue.

I'm using a really simple validation, counting the unique ids from both the input data and the saved parquet files. Ideally we could have some metadata service that could validate both the input and output schema with ease and even sample some rows from the input to validate against our output.

## DataMarts

I'm still using AWS S3 for the same reasons as explained above.

### Orders

I've deduplicated the records using the 'order_id' as subset and removed both 'cpf' and 'customer_name' from the orders dataset and 'customer_phone' from the consumers dataset in order to keep the data anonymized. We can still analyze unique customers by using the 'customer_id'.
I decided to rename a lot of columns in order to clarify where they came from (order, customer or restaurant)

I've also decided to use 'left' joins in order to show data issues we might have (for instance, some 'order_ids') without 'status'. A further improvement would be to flag those records so they would be easier to filter.

The dataset is partitioned by the order created_at at the restaurant local timezone.

### Order Statuses

It's a really simple pivot in order to show the needed dates.

### Order Items

This one took me a while (probably a few hours) because I couldn't make the json parsing work. Eventually I decided to manually fix the schema for it to work. After fixing the schema is pretty much straight forward.

## Observations

* The repartition values are arbitraty for those specific datasets. I wanted to fix the partition size and not the number of partitions, but the block size configuration wasn't working for me. I need more time to further study if it's possible to do that with PySpark, if I need to migrate to Scala or if it's not possible anymore. (Other solution would be to estimate the dataframe size and dinamically choose a repartition value based on it).

* The current script overwrites the data everytime it runs. A great improvement would be to do it incrementally. In order to do that, I'd partition every raw dataset using a timestamp column (such as created_at or updated_at in other cases) in order to know which partition to search for the records to insert or updated if needed. This would require some metadata control to know for instance the "primary key" of the dataset, to facilitate the searching.

* As mentioned above, there isn't enough validation at the raw layer and there's none at the datamarts. Following the suggestion above would be a needed improvement.

