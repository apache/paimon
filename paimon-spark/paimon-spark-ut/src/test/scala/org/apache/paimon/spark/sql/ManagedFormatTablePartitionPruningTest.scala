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

package org.apache.paimon.spark.sql

import org.apache.paimon.predicate.{LeafPredicate, Predicate, PredicateBuilder}
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.utils.JsonSerdeUtil

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/**
 * End-to-end guard of the pushdown chain: a Spark SQL partition predicate on a catalog-managed
 * Format Table must reach the REST `partitions/list-by-filter` endpoint, not only produce correct
 * rows (the client-side residual filter would mask a broken transport path).
 */
class ManagedFormatTablePartitionPruningTest extends PaimonSparkTestWithRestCatalogBase {

  test("Format Table: partition predicate reaches the filtered listing endpoint") {
    val tableName = "pruning_endpoint_t"
    withTable(tableName) {
      sql(s"""CREATE TABLE $tableName (id INT, payload STRING, year INT, month INT)
             |USING CSV
             |PARTITIONED BY (year, month)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'true')
             |""".stripMargin)
      sql(s"""INSERT INTO $tableName VALUES
             |(1, 'a', 2024, 12), (2, 'b', 2025, 9), (3, 'c', 2025, 11), (4, 'd', 2025, 12)
             |""".stripMargin)

      restCatalogServer.receivedListPartitionsByFilterRequests.clear()
      checkAnswer(
        sql(s"SELECT id FROM $tableName WHERE year = 2025 AND month > 10 ORDER BY id"),
        Seq(Row(3), Row(4)))

      val requests = restCatalogServer.receivedListPartitionsByFilterRequests
      assert(!requests.isEmpty, "the filtered listing endpoint was not called")
      val request = requests.get(requests.size() - 1)
      assert(request.getFilter != null && request.getFilter.nonEmpty)
      assert(
        request.getPartitionNamePattern != null
          && request.getPartitionNamePattern.startsWith("year=2025"),
        s"unexpected pattern ${request.getPartitionNamePattern}"
      )

      // The filter restores to the pushed predicate (the wire form round-trips) and uses the
      // flat encoding, no pretty-print newlines.
      val restored = JsonSerdeUtil.fromJson(request.getFilter, classOf[Predicate])
      val leafFields = PredicateBuilder
        .splitAnd(restored)
        .asScala
        .collect { case leaf: LeafPredicate => leaf.fieldNames().asScala }
        .flatten
        .toSet
      assert(
        leafFields.contains("year") && leafFields.contains("month"),
        s"restored filter misses pushed fields: $restored")
      assert(!request.getFilter.contains("\n"), "filter is not flat-encoded")
    }
  }
}
