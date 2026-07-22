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

package org.apache.paimon.spark.format

import org.apache.paimon.spark.{PaimonSparkTestWithRestCatalogBase, SparkCatalog}

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier => SparkIdentifier}

class CatalogManagedPartitionTest extends PaimonSparkTestWithRestCatalogBase {

  test(
    "a Format Table with catalog-managed partitions loaded through SparkCatalog carries its partition manager") {
    val tableName = "catalog_partition_format_table_load"
    withTable(tableName) {
      sql(
        s"CREATE TABLE $tableName (id INT, dt STRING) USING CSV " +
          "TBLPROPERTIES ('format-table.implementation'='paimon', " +
          "'metastore.partitioned-table'='true') PARTITIONED BY (dt)")

      val sparkCatalog = spark.sessionState.catalogManager
        .catalog("paimon")
        .asInstanceOf[SparkCatalog]
      val sparkTable = sparkCatalog
        .loadTable(SparkIdentifier.of(Array(dbName0), tableName))
        .asInstanceOf[PaimonFormatTable]

      assert(sparkTable.partitionManager != null)
    }
  }

  test("catalog-managed Format Table sends partition predicate to REST filtered listing") {
    val tableName = "catalog_partition_filter_pushdown"
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

      clearFilteredListingRequests()
      checkAnswer(
        sql(s"SELECT id FROM $tableName WHERE year = 2025 AND month > 10 ORDER BY id"),
        Seq(Row(3), Row(4)))

      assert(filteredListingWasRequested, "the filtered listing endpoint was not called")
    }
  }
}
