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

import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

/** Verifies chain branch reads through a versioned REST catalog when main has no snapshot. */
class ChainTableBranchReadWithRestCatalogTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.catalog.paimon.cache-enabled", "false")
  }

  Seq(false, true).foreach {
    legacy =>
      test(s"read chain branches through REST catalog, legacy=$legacy") {
        val table = s"chain_rest_branch_$legacy"
        withTable(table) {
          sql(s"""
                 |CREATE TABLE $table (id BIGINT, seq BIGINT, value STRING)
                 |PARTITIONED BY (dt STRING)
                 |TBLPROPERTIES (
                 | 'chain-table.enabled'='true', 'primary-key'='dt,id',
                 | 'sequence.field'='seq', 'bucket-key'='id', 'bucket'='1',
                 | 'partition.legacy-name'='$legacy',
                 | 'partition.timestamp-pattern'='$$dt',
                 | 'partition.timestamp-formatter'='yyyy-MM-dd')
                 |""".stripMargin)
          sql(s"CALL sys.create_branch('$table', 'snapshot')")
          sql(s"CALL sys.create_branch('$table', 'delta')")
          Seq(table, s"$table$$branch_snapshot", s"$table$$branch_delta").foreach {
            name =>
              sql(s"""ALTER TABLE `$name` SET TBLPROPERTIES (
                     | 'scan.fallback-snapshot-branch'='snapshot',
                     | 'scan.fallback-delta-branch'='delta')""".stripMargin)
          }
          sql(
            s"INSERT INTO `$table$$branch_snapshot` PARTITION (dt='2025-01-01') VALUES (1,1,'old'),(2,1,'keep')")
          sql(
            s"INSERT INTO `$table$$branch_delta` PARTITION (dt='2025-01-02') VALUES (1,2,'new'),(3,1,'add')")
          assert(loadTable(table).snapshotManager().latestSnapshot() == null)

          def chainRows = sql(s"SELECT id,seq,value FROM $table WHERE dt='2025-01-02'")
          checkAnswer(chainRows, Seq(Row(1L, 2L, "new"), Row(2L, 1L, "keep"), Row(3L, 1L, "add")))
          sql(
            s"INSERT INTO `$table$$branch_delta` PARTITION (dt='2025-01-02') VALUES (2,2,'updated')")
          checkAnswer(
            chainRows,
            Seq(Row(1L, 2L, "new"), Row(2L, 2L, "updated"), Row(3L, 1L, "add")))
          checkAnswer(
            sql(s"SELECT id,seq,value FROM `$table$$branch_snapshot` WHERE dt='2025-01-01'"),
            Seq(Row(1L, 1L, "old"), Row(2L, 1L, "keep")))
          checkAnswer(
            sql(s"SELECT id,seq,value FROM `$table$$branch_delta` WHERE dt='2025-01-02'"),
            Seq(Row(1L, 2L, "new"), Row(2L, 2L, "updated"), Row(3L, 1L, "add"))
          )
          assert(loadTable(table).snapshotManager().latestSnapshot() == null)
        }
      }
  }
}
