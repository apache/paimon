/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.catalyst.plans.logical

import org.apache.paimon.globalindex.EsSearchQuery
import org.apache.paimon.spark.query.SparkEsSearchRequest

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.types._

/** Literal envelope resolution performs no catalog IO or index reader creation. */
case class PaimonEsSearchQuery(override val args: Seq[Expression])
  extends PaimonTableValueFunction(PaimonTableValuedFunctions.PAIMON_ES_SEARCH) {
  override def parseArgs(ignored: Seq[Expression]): Map[String, String] = Map.empty
  def resolveSearch(): PaimonEsSearchRelation = {
    if (
      args.size != 2 || args.exists(
        value => !value.resolved || !value.foldable || value.dataType != StringType)
    ) {
      throw new IllegalArgumentException(
        "paimon_es_search requires two constant STRING arguments: catalog_name, query_json")
    }
    def text(index: Int): String = {
      val value = args(index).eval()
      if (value == null)
        throw new IllegalArgumentException("paimon_es_search arguments must not be null")
      value.toString
    }
    val catalog = text(0)
    val envelope = text(1)
    if (
      catalog.isEmpty || catalog.length > 1024 || catalog.indexOf('\u0000') >= 0 ||
      envelope.isEmpty || envelope.length > EsSearchQuery.MAX_BYTES / 3 || envelope.indexOf(
        '\u0000') >= 0
    ) {
      throw new IllegalArgumentException("paimon_es_search argument size or text is invalid")
    }
    SparkEsSearchRequest.parse(envelope)
    PaimonEsSearchRelation(catalog, envelope, PaimonEsSearchRelation.attributes())
  }
}

/** A resolved bounded result table; catalog planning and index reads start only at execution. */
case class PaimonEsSearchRelation(
    catalogName: String,
    envelope: String,
    override val output: Seq[Attribute])
  extends LeafNode

object PaimonEsSearchRelation {
  def attributes(): Seq[Attribute] = Seq(
    AttributeReference("record_kind", StringType, nullable = false)(),
    AttributeReference("query_id", StringType, nullable = false)(),
    AttributeReference("query_ordinal", IntegerType, nullable = false)(),
    AttributeReference("execution_query_id", StringType, nullable = false)(),
    AttributeReference("target", StringType)(),
    AttributeReference("table_name", StringType)(),
    AttributeReference("branch", StringType)(),
    AttributeReference("snapshot_id", LongType)(),
    AttributeReference("row_id", LongType)(),
    AttributeReference("score", FloatType)(),
    AttributeReference("rank", LongType)(),
    AttributeReference("sort_values_json", StringType)(),
    AttributeReference("source_schema_json", StringType)(),
    AttributeReference("source_binary", BinaryType)(),
    AttributeReference("source_encoding", StringType)(),
    AttributeReference("metadata_json", StringType, nullable = false)(),
    AttributeReference("index_fields_json", StringType)()
  )
}
