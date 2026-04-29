#!/usr/bin/env python3
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("show-lineage-events").getOrCreate()

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW lineage_events
        USING delta
        OPTIONS (path 'demo/results/deltalake/lineage-events')
        """
    )

    spark.sql(
        """
        SELECT
          event_kind,
          event_type,
          job_namespace,
          job_name,
          run_id,
          event_time
        FROM lineage_events
        ORDER BY event_time DESC
        LIMIT 20
        """
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
