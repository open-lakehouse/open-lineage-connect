from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("process_lineage_events") \
    .getOrCreate()

dataset_schema = ArrayType(StructType([
    StructField("name", StringType()),
    StructField("namespace", StringType())
]))

BASE_PATH = "/delta/flattened_lineage_tables"

df = spark.read.format("delta").load("/delta/lineage-events/event_kind=run")

# Jobs table - run_id as unique vertex ID
jobs = df.select(
    "run_id",
    "job_name",
    "job_namespace",
    "event_time",
    "event_type",
    "producer"
).distinct()

# Parse JSON string columns
df_parsed = df.withColumn("inputs", from_json(col("inputs_json"), dataset_schema)) \
              .withColumn("outputs", from_json(col("outputs_json"), dataset_schema))

# Edge table - job to input datasets
job_inputs = df_parsed.filter(col("inputs").isNotNull()).select(
    "run_id",
    "job_name",
    "job_namespace",
    "event_time",
    "event_type",
    explode("inputs").alias("input")
).select(
    "run_id",
    "job_name",
    "job_namespace",
    "event_time",
    "event_type",
    col("input.name").alias("dataset_name"),
    col("input.namespace").alias("dataset_namespace")
)

# Edge table - job to output datasets
job_outputs = df_parsed.filter(col("outputs").isNotNull()).select(
    "run_id",
    "job_name",
    "job_namespace",
    "event_time",
    "event_type",
    explode("outputs").alias("output")
).select(
    "run_id",
    "job_name",
    "job_namespace",
    "event_time",
    "event_type",
    col("output.name").alias("dataset_name"),
    col("output.namespace").alias("dataset_namespace")
)

# Write tables
jobs.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/jobs")
job_inputs.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/job_inputs")
job_outputs.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/job_outputs")
