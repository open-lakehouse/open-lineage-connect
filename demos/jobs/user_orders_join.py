#!/usr/bin/env python3
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    orders_path = os.environ["DEMO_ORDERS_PATH"]
    users_path = os.environ["DEMO_USERS_PATH"]
    user_orders_path = os.environ["DEMO_USER_ORDERS_PATH"]

    spark = SparkSession.builder.appName("open-lakehouse-user-orders-demo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Warm-up query so deferred plugin listener registration can attach before
    # the lineage-producing read/join/write workload starts.
    spark.range(1).count()

    orders_df = spark.read.format("delta").load(orders_path)
    users_df = spark.read.format("delta").load(users_path)
    # Force query execution so the plugin listener observes read plans.
    orders_df.count()
    users_df.count()

    user_orders_df = (
        orders_df.alias("o")
        .join(users_df.alias("u"), F.col("o.user_id") == F.col("u.uuid"), "inner")
        .select(
            F.col("o.user_id").alias("user_id"),
            F.col("u.first_name").alias("first_name"),
            F.col("u.last_name").alias("last_name"),
            F.col("u.email_address").alias("email_address"),
            F.col("o.created_at").alias("order_created_at"),
            F.col("o.updated_at").alias("order_updated_at"),
            F.col("o.products").alias("products"),
            F.col("o.total").alias("total"),
        )
    )

    # Force the join query to execute prior to write.
    user_orders_df.count()
    user_orders_df.write.format("delta").mode("overwrite").save(user_orders_path)
    spark.stop()


if __name__ == "__main__":
    main()
