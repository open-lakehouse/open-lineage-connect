package com.openlakehouse.lineage.driver

/**
 * A lineage-bound reference to a dataset (either a source or a sink).
 *
 * `namespace` + `name` is the identity we propagate into the OpenLineage
 * `InputDataset` / `OutputDataset` proto messages; `sourceType` is a free-form
 * hint (format/provider) useful as a facet.
 *
 * Two refs are considered equal when (`namespace`, `name`) match — `sourceType`
 * and `facets` are metadata that can vary across occurrences without breaking
 * lineage identity.
 */
final case class DatasetRef(
    namespace: String,
    name: String,
    sourceType: String,
    facets: Map[String, String] = Map.empty
) {
  def identityKey: (String, String) = (namespace, name)
}

object DatasetRef {

  /** Deduplicate a sequence of refs by (namespace, name), keeping first occurrence. */
  def distinctByIdentity(refs: Seq[DatasetRef]): Seq[DatasetRef] = {
    val seen = scala.collection.mutable.HashSet.empty[(String, String)]
    val out  = Vector.newBuilder[DatasetRef]
    refs.foreach { r =>
      if (seen.add(r.identityKey)) out += r
    }
    out.result()
  }
}
