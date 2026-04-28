# Spark 4.1.1 Catalyst API Audit

This document records every Catalyst/SQL API the plugin touches, verified against **Apache Spark 4.1.1** (Scala 2.13.17, released 2026-01-02). The intent is a stable reference we can diff against when bumping Spark versions — any signature change must be reflected in the `QueryPlanVisitor` implementation.

**Method:** `javap -p` against jars resolved from Maven Central:
- `spark-catalyst_2.13-4.1.1.jar`
- `spark-sql_2.13-4.1.1.jar`
- `spark-sql-api_2.13-4.1.1.jar`

## Listener hooks (public API — binary-compatible within a Spark major)

| Hook                           | Package                                                   | Notes                                   |
|--------------------------------|-----------------------------------------------------------|-----------------------------------------|
| `SparkPlugin`                  | `org.apache.spark.api.plugin`                             | Registered via `spark.plugins` conf     |
| `DriverPlugin`                 | `org.apache.spark.api.plugin`                             | `init(SparkContext, PluginContext): java.util.Map[String,String]` |
| `ExecutorPlugin`               | `org.apache.spark.api.plugin`                             | `init(PluginContext, java.util.Map)`    |
| `PluginContext`                | `org.apache.spark.api.plugin`                             | `send(Any)`, `ask(Any): AnyRef`         |
| `QueryExecutionListener`       | `org.apache.spark.sql.util`                               | Register via `spark.listenerManager`    |
| `SparkListener`                | `org.apache.spark.scheduler`                              | Register via `sc.addSparkListener`      |
| `StreamingQueryListener`       | `org.apache.spark.sql.streaming`                          | Register via `spark.streams.addListener`|

## Sources (leaf reads)

| Node                          | Package                                                | Accessors we use                                         |
|-------------------------------|--------------------------------------------------------|----------------------------------------------------------|
| `LogicalRelation`             | `org.apache.spark.sql.execution.datasources`           | `relation: BaseRelation`, `catalogTable: Option[CatalogTable]`, `isStreaming: Boolean`, `stream: Option[SparkDataStream]` |
| `HiveTableRelation`           | `org.apache.spark.sql.catalyst.catalog`                | `tableMeta: CatalogTable` → `.identifier.database/.table` |
| `DataSourceV2Relation`        | `org.apache.spark.sql.execution.datasources.v2`        | `table: Table`, `identifier: Option[Identifier]`, `catalog: Option[CatalogPlugin]`, `options: CaseInsensitiveStringMap` |
| `DataSourceV2ScanRelation`    | `org.apache.spark.sql.execution.datasources.v2`        | `relation: DataSourceV2Relation`, `scan: Scan` — extends `NamedRelation` |
| `UnresolvedRelation`          | `org.apache.spark.sql.catalyst.analysis`               | Seen only on the **unresolved** plan — we listen on `qe.analyzed` so this should not normally appear. Kept as a fallback match. |

### Supporting types

| Type                                          | Package                                                   | Relevant accessors                     |
|-----------------------------------------------|-----------------------------------------------------------|----------------------------------------|
| `NamedRelation` (trait)                       | `org.apache.spark.sql.catalyst.analysis`                  | `name(): String`                       |
| `Identifier` (interface)                      | `org.apache.spark.sql.connector.catalog`                  | `namespace(): Array[String]`, `name(): String` |
| `Table` (interface)                           | `org.apache.spark.sql.connector.catalog`                  | `name(): String`, `schema(): StructType` |
| `CatalogPlugin` (interface)                   | `org.apache.spark.sql.connector.catalog`                  | `name(): String`                       |
| `CatalogTable`                                | `org.apache.spark.sql.catalyst.catalog`                   | `identifier: TableIdentifier`, `provider: Option[String]`, `storage.locationUri: Option[URI]` |
| `BaseRelation` (abstract)                     | `org.apache.spark.sql.sources`                            | Per-source; `HadoopFsRelation.location.rootPaths` for file-based |
| `HadoopFsRelation`                            | `org.apache.spark.sql.execution.datasources`              | `location: FileIndex`, `fileFormat: FileFormat` |

## Sinks (write commands)

### V1 (DataSource v1 / file-based / JDBC)

| Node                                      | Package                                             | Accessors we use                                      |
|-------------------------------------------|-----------------------------------------------------|-------------------------------------------------------|
| `InsertIntoHadoopFsRelationCommand`       | `org.apache.spark.sql.execution.datasources`        | `outputPath: org.apache.hadoop.fs.Path`, `catalogTable: Option[CatalogTable]`, `fileFormat: FileFormat`, `mode: SaveMode` |
| `SaveIntoDataSourceCommand`               | `org.apache.spark.sql.execution.datasources`        | `dataSource: CreatableRelationProvider`, `options: Map[String,String]` (look for `"path"` key), `mode: SaveMode` |
| `CreateDataSourceTableAsSelectCommand`    | `org.apache.spark.sql.execution.command`            | `table: CatalogTable`, `mode: SaveMode`, `outputColumnNames: Seq[String]` |
| `CreateHiveTableAsSelectCommand`          | *(in `spark-hive_2.13`, not `spark-sql`)*           | Only available when Hive support is enabled — match **reflectively**. |

### V2 (DataSource v2 — Delta, Iceberg, Kafka-as-batch, etc.)

All of these live in `org.apache.spark.sql.catalyst.plans.logical` and share `.table: NamedRelation` (which exposes `.name(): String`), `.query: LogicalPlan`, and `.writeOptions: Map[String, String]`.

| Node                        | Additional accessors                                                              |
|-----------------------------|-----------------------------------------------------------------------------------|
| `AppendData`                | `isByName: Boolean`, `write: Option[Write]`, `analyzedQuery: Option[LogicalPlan]` |
| `OverwriteByExpression`     | `deleteExpr: Expression`                                                          |
| `OverwritePartitionsDynamic`| *(same shape as AppendData, dynamic partition semantics)*                         |
| `ReplaceData`               | *(delta-flavored data replacement; not all connectors emit this)*                 |
| `CreateTableAsSelect`       | `name: LogicalPlan` (a `ResolvedIdentifier` after analysis), `partitioning: Seq[Transform]`, `tableSpec: TableSpecBase`, `ignoreIfExists: Boolean` |
| `ReplaceTableAsSelect`      | *(same shape as CreateTableAsSelect, with replace semantics)*                     |

### Lower-level v2 writes

| Node                              | Package                                                     | Notes                                                               |
|-----------------------------------|-------------------------------------------------------------|---------------------------------------------------------------------|
| `WriteToDataSourceV2`             | `org.apache.spark.sql.execution.datasources.v2` *(in `spark-sql`)* | Lower-level write; `relation: Option[DataSourceV2Relation]`, `batchWrite: BatchWrite`, `query: LogicalPlan` |
| `WriteToMicroBatchDataSource`     | `org.apache.spark.sql.execution.streaming.sources`          | Streaming microbatch sink wrapper                                   |

### Optional / reflective matches (only if corresponding lib is on the classpath)

These classes are **not** in `spark-sql_2.13`. The visitor must use `Class.forName(..., false, classLoader)` inside a `scala.util.Try` and match on class name equality, so the plugin does not hard-depend on Delta/Iceberg/Hudi.

| Class                                                     | Provided by                           |
|-----------------------------------------------------------|---------------------------------------|
| `org.apache.spark.sql.delta.commands.WriteIntoDelta`      | `io.delta:delta-spark_2.13:4.0.0`     |
| `org.apache.spark.sql.delta.commands.MergeIntoCommand`    | same                                  |
| `org.apache.spark.sql.delta.commands.DeleteCommand`       | same                                  |
| `org.apache.spark.sql.delta.commands.UpdateCommand`       | same                                  |
| `org.apache.iceberg.spark.source.SparkWrite$*`            | `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13` |
| `org.apache.hudi.spark.*WriteSource`                      | `org.apache.hudi:hudi-spark4-bundle_2.13` |

## Column-lineage extractor surface

`ColumnLineageExtractor` walks the analyzed `LogicalPlan` to derive the typed
`ColumnLineageDatasetFacet`. Every Catalyst class touched is listed here so
the same audit cadence applies on Spark version bumps.

### Catalyst expressions

| Type                                           | Package                                              | What we read                                  |
|------------------------------------------------|------------------------------------------------------|-----------------------------------------------|
| `AttributeReference`                           | `org.apache.spark.sql.catalyst.expressions`          | `name: String`, `exprId: ExprId`              |
| `Alias`                                        | `org.apache.spark.sql.catalyst.expressions`          | `child: Expression`, `name: String`           |
| `NamedExpression`                              | `org.apache.spark.sql.catalyst.expressions`          | `name: String`, `toAttribute: Attribute`      |
| `Expression.references: AttributeSet`          | `org.apache.spark.sql.catalyst.expressions`          | Walked to attribute exprIds for lineage seeds |
| `AggregateExpression`                          | `org.apache.spark.sql.catalyst.expressions.aggregate`| Wraps `AggregateFunction`; classified as `DIRECT/AGGREGATION` |
| `Md5` / `Sha1` / `Sha2` / `Mask` / `RegExpReplace` | `org.apache.spark.sql.catalyst.expressions`     | Heuristic for `masking=true` flag             |

### Catalyst logical operators (in addition to those listed above)

| Node                                  | Package                                            | What we read                                                      |
|---------------------------------------|----------------------------------------------------|-------------------------------------------------------------------|
| `Project`                             | `org.apache.spark.sql.catalyst.plans.logical`      | `projectList: Seq[NamedExpression]`                               |
| `Aggregate`                           | same                                               | `aggregateExpressions`, `groupingExpressions`                     |
| `Window`                              | same                                               | `windowExpressions`, `partitionSpec`, `orderSpec`                 |
| `Generate`                            | same                                               | `generator: Generator`, `generatorOutput`                         |
| `SubqueryAlias`                       | same                                               | propagates child lineage; only `alias` matters                    |
| `Filter`                              | same                                               | `condition: Expression` (INDIRECT/FILTER)                         |
| `Sort`                                | same                                               | `order: Seq[SortOrder]` (INDIRECT/SORT)                           |
| `Join`                                | same                                               | `condition: Option[Expression]` (INDIRECT/JOIN)                   |
| `Union`                               | same                                               | merges child lineage on positional output                         |

The extractor calls `Expression.references` and `LogicalPlan.output` only —
both are part of the public Catalyst surface and stable within a major.

## Key observations / risks

1. **`NamedRelation.name(): String` is the unifying accessor for v2 writes.** Every `V2WriteCommand` (AppendData, OverwriteByExpression, etc.) holds its target as a `NamedRelation`, which exposes `.name()`. That's the simplest path to a dataset name without caring whether the underlying type is `DataSourceV2Relation`, `DataSourceV2ScanRelation`, or a connector-specific subclass.

2. **`DataSourceV2Relation.identifier` is `Option[Identifier]`.** It may be `None` when the relation is synthesized without a catalog (e.g., `spark.read.format("iceberg").load("<path>")`). In that case fall back to `.table.name()` or `.options.get("path")`.

3. **`LogicalRelation.catalogTable` is `Option[CatalogTable]`.** Populated for reads of metastore-registered tables (`spark.table("foo")`); `None` for path-based reads (`spark.read.parquet("<path>")`). For path-based, inspect `.relation` — if `HadoopFsRelation`, read `.location.rootPaths`.

4. **`CreateTableAsSelect.name: LogicalPlan` — not a string, not an Identifier.** After analysis this is a `ResolvedIdentifier` whose `.identifier` field gives the real `Identifier`. Walk it via pattern match rather than assuming the concrete subtype.

5. **`CreateHiveTableAsSelectCommand` is in `spark-hive_2.13`**, not in `spark-sql_2.13`. Users without the Hive module on their classpath won't have the class. Always match reflectively.

6. **These are SQL-internal APIs with no binary-compatibility guarantee.** Spark's `DeveloperApi`/`InterfaceStability` annotations do not cover these logical-plan node classes. Any minor upgrade (4.1.1 → 4.2.0) could rename, move, or change field signatures. Re-run this audit on every Spark major/minor bump and update `QueryPlanVisitor` if needed.

7. **Streaming plans:** `StreamingQueryListener.onQueryProgress` doesn't hand you a `LogicalPlan`. To introspect the plan of a microbatch, reach through `queryExecution` via `spark.streams.get(runId).lastExecution: IncrementalExecution`, which extends `QueryExecution`.

## Classes confirmed present in Spark 4.1.1 jars

```
✓ org.apache.spark.sql.execution.datasources.LogicalRelation                    (spark-sql)
✓ org.apache.spark.sql.catalyst.catalog.HiveTableRelation                       (spark-catalyst)
✓ org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation            (spark-catalyst)
✓ org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation        (spark-catalyst)
✓ org.apache.spark.sql.catalyst.analysis.UnresolvedRelation                     (spark-catalyst)
✓ org.apache.spark.sql.catalyst.analysis.NamedRelation                          (spark-catalyst)
✓ org.apache.spark.sql.connector.catalog.Identifier                             (spark-sql-api)
✓ org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand  (spark-sql)
✓ org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand          (spark-sql)
✓ org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand   (spark-sql)
✓ org.apache.spark.sql.catalyst.plans.logical.AppendData                        (spark-catalyst)
✓ org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression             (spark-catalyst)
✓ org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic        (spark-catalyst)
✓ org.apache.spark.sql.catalyst.plans.logical.ReplaceData                       (spark-catalyst)
✓ org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect               (spark-catalyst)
✓ org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect              (spark-catalyst)
✓ org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2             (spark-sql)
✓ org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSource  (spark-sql)

✗ org.apache.spark.sql.execution.command.CreateHiveTableAsSelectCommand         (→ spark-hive only, match reflectively)
```

Verified on: 2026-04-21.
