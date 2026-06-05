package com.openlakehouse.lineage.driver

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType,
  HiveTableRelation
}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Exercises the `HiveTableRelation` source extraction path
 * (`QueryPlanVisitor.fromHiveTableRelation`) without standing up a Hive
 * metastore. We construct a `HiveTableRelation` directly from a `CatalogTable`,
 * which is exactly the node Spark plants in an analyzed plan when reading a
 * Hive-managed table — so this gives deterministic coverage of the code path
 * that previously only ran (incidentally) against a real Hive-enabled Spark.
 */
final class QueryPlanVisitorHiveSpec extends AnyFunSuite with Matchers {

  private def hiveRelation(
      database: String,
      table: String,
      provider: Option[String]
  ): HiveTableRelation = {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
    val catalogTable = CatalogTable(
      identifier = TableIdentifier(table, Some(database)),
      tableType  = CatalogTableType.MANAGED,
      storage    = CatalogStorageFormat.empty,
      schema     = schema,
      provider   = provider
    )
    val dataCols = schema.fields.toSeq.map(f => AttributeReference(f.name, f.dataType, f.nullable)())
    HiveTableRelation(catalogTable, dataCols, partitionCols = Seq.empty)
  }

  test("extractSources lifts a HiveTableRelation into a hive-catalog DatasetRef") {
    val rel     = hiveRelation("salesdb", "customers", provider = Some("hive"))
    val sources = QueryPlanVisitor.extractSources(rel)

    sources should have size 1
    val dr = sources.head
    dr.namespace shouldBe "salesdb"
    dr.name      shouldBe "customers"
    dr.sourceType shouldBe "hive"
    dr.facets.get("catalog") shouldBe Some("hive")
  }

  test("fromHiveTableRelation defaults sourceType to 'hive' when provider is unset") {
    val rel = hiveRelation("default", "events", provider = None)
    val dr  = QueryPlanVisitor.fromHiveTableRelation(rel)

    dr.namespace shouldBe "default"
    dr.name      shouldBe "events"
    dr.sourceType shouldBe "hive"
    dr.facets.get("catalog") shouldBe Some("hive")
  }
}
