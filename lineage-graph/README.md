# Lineage Graph Demo

## Summary

This demo showcases how to capture, store, and query OpenLineage events as a graph using PuppyGraph.

By modeling data lineage as a graph, users can identify and traverse relationships between jobs and datasets, such as:
- Which datasets were read or written by a given Spark job
- How the output of one job becomes the input of another
- Full upstream/downstream impact analysis across a pipeline

Using Cypher queries, users can traverse the lineage graph to trace data flows end-to-end. This practical approach assists in understanding data provenance and dependency analysis in an open lakehouse environment.

## Demos

- [`deltalake/`](./deltalake/README.md) — Delta Lake + OSS Unity Catalog
- [`iceberg/`](./iceberg/README.md) — Apache Iceberg + Iceberg REST catalog