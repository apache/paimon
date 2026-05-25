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

package org.apache.paimon.spark.procedure

import org.apache.paimon.Snapshot.CommitKind
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable

import org.assertj.core.api.Assertions

/** Test compact_database procedure. See [[CompactDatabaseProcedure]]. */
class CompactDatabaseProcedureTest extends PaimonSparkTestBase {

  def lastSnapshotCommand(table: FileStoreTable): CommitKind = {
    table.snapshotManager().latestSnapshot().commitKind()
  }

  def lastSnapshotId(table: FileStoreTable): Long = {
    table.snapshotManager().latestSnapshotId()
  }

  test("Paimon Procedure: compact database - basic test") {
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db1")
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db2")

    withTable("test_db1.T1", "test_db1.T2", "test_db2.T3") {
      // Create tables in test_db1
      spark.sql(s"""
                   |CREATE TABLE test_db1.T1 (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true')
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE test_db1.T2 (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true')
                   |""".stripMargin)

      // Create table in test_db2
      spark.sql(s"""
                   |CREATE TABLE test_db2.T3 (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true')
                   |""".stripMargin)

      // Insert data multiple times to create multiple files that need compaction
      spark.sql("INSERT INTO test_db1.T1 VALUES (1, 'a'), (2, 'b')")
      spark.sql("INSERT INTO test_db1.T1 VALUES (3, 'c'), (4, 'd')")

      spark.sql("INSERT INTO test_db1.T2 VALUES (1, 'x'), (2, 'y')")
      spark.sql("INSERT INTO test_db1.T2 VALUES (3, 'z'), (4, 'w')")

      spark.sql("INSERT INTO test_db2.T3 VALUES (1, 'm'), (2, 'n')")
      spark.sql("INSERT INTO test_db2.T3 VALUES (3, 'o'), (4, 'p')")

      val table1 = loadTable("test_db1", "T1")
      val table2 = loadTable("test_db1", "T2")
      val table3 = loadTable("test_db2", "T3")

      Assertions.assertThat(lastSnapshotId(table1)).isEqualTo(2)
      Assertions.assertThat(lastSnapshotId(table2)).isEqualTo(2)
      Assertions.assertThat(lastSnapshotId(table3)).isEqualTo(2)

      // Compact all databases
      val result = spark.sql("CALL sys.compact_database()").collect()
      Assertions.assertThat(result.length).isEqualTo(1)
      Assertions.assertThat(result(0).getString(0)).contains("Success")

      // Verify compaction happened - reload table to get new snapshot
      val table1After = loadTable("test_db1", "T1")
      val table2After = loadTable("test_db1", "T2")
      val table3After = loadTable("test_db2", "T3")
      Assertions.assertThat(lastSnapshotCommand(table1After).equals(CommitKind.COMPACT)).isTrue
      Assertions.assertThat(lastSnapshotCommand(table2After).equals(CommitKind.COMPACT)).isTrue
      Assertions.assertThat(lastSnapshotCommand(table3After).equals(CommitKind.COMPACT)).isTrue
    }

    spark.sql("DROP DATABASE IF EXISTS test_db1 CASCADE")
    spark.sql("DROP DATABASE IF EXISTS test_db2 CASCADE")
  }

  test("Paimon Procedure: compact database - with database filter") {
    spark.sql("CREATE DATABASE IF NOT EXISTS db_include")
    spark.sql("CREATE DATABASE IF NOT EXISTS db_exclude")

    withTable("db_include.T1", "db_exclude.T2") {
      spark.sql(s"""
                   |CREATE TABLE db_include.T1 (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true')
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE db_exclude.T2 (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true')
                   |""".stripMargin)

      spark.sql("INSERT INTO db_include.T1 VALUES (1, 'a'), (2, 'b')")
      spark.sql("INSERT INTO db_include.T1 VALUES (3, 'c'), (4, 'd')")

      spark.sql("INSERT INTO db_exclude.T2 VALUES (1, 'x'), (2, 'y')")
      spark.sql("INSERT INTO db_exclude.T2 VALUES (3, 'z'), (4, 'w')")

      val table1 = loadTable("db_include", "T1")
      val table2 = loadTable("db_exclude", "T2")

      // Compact only db_include database
      spark.sql("CALL sys.compact_database(including_databases => 'db_include')")

      // Only table1 should be compacted - reload table to get new snapshot
      val table1After = loadTable("db_include", "T1")
      Assertions.assertThat(lastSnapshotCommand(table1After).equals(CommitKind.COMPACT)).isTrue
      Assertions.assertThat(lastSnapshotId(table2)).isEqualTo(2) // Still only 2 snapshots
    }

    spark.sql("DROP DATABASE IF EXISTS db_include CASCADE")
    spark.sql("DROP DATABASE IF EXISTS db_exclude CASCADE")
  }

  test("Paimon Procedure: compact database - with table filter") {
    spark.sql("CREATE DATABASE IF NOT EXISTS filter_db")

    withTable("filter_db.include_table", "filter_db.exclude_table") {
      spark.sql(s"""
                   |CREATE TABLE filter_db.include_table (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true')
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE filter_db.exclude_table (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true')
                   |""".stripMargin)

      spark.sql("INSERT INTO filter_db.include_table VALUES (1, 'a'), (2, 'b')")
      spark.sql("INSERT INTO filter_db.include_table VALUES (3, 'c'), (4, 'd')")

      spark.sql("INSERT INTO filter_db.exclude_table VALUES (1, 'x'), (2, 'y')")
      spark.sql("INSERT INTO filter_db.exclude_table VALUES (3, 'z'), (4, 'w')")

      val includeTable = loadTable("filter_db", "include_table")
      val excludeTable = loadTable("filter_db", "exclude_table")

      // Compact only include_table using including_tables pattern
      spark.sql(
        "CALL sys.compact_database(including_databases => 'filter_db', including_tables => '.*include.*')")

      val includeTableAfter = loadTable("filter_db", "include_table")
      Assertions
        .assertThat(lastSnapshotCommand(includeTableAfter).equals(CommitKind.COMPACT))
        .isTrue
      Assertions.assertThat(lastSnapshotId(excludeTable)).isEqualTo(2)
    }

    spark.sql("DROP DATABASE IF EXISTS filter_db CASCADE")
  }

  test("Paimon Procedure: compact database - with excluding_tables filter") {
    spark.sql("CREATE DATABASE IF NOT EXISTS exclude_db")

    withTable("exclude_db.T1", "exclude_db.T2") {
      spark.sql(s"""
                   |CREATE TABLE exclude_db.T1 (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true')
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE exclude_db.T2 (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true')
                   |""".stripMargin)

      spark.sql("INSERT INTO exclude_db.T1 VALUES (1, 'a'), (2, 'b')")
      spark.sql("INSERT INTO exclude_db.T1 VALUES (3, 'c'), (4, 'd')")

      spark.sql("INSERT INTO exclude_db.T2 VALUES (1, 'x'), (2, 'y')")
      spark.sql("INSERT INTO exclude_db.T2 VALUES (3, 'z'), (4, 'w')")

      val table1 = loadTable("exclude_db", "T1")
      val table2 = loadTable("exclude_db", "T2")

      // Compact all tables except T2
      spark.sql(
        "CALL sys.compact_database(including_databases => 'exclude_db', excluding_tables => '.*T2')")

      val table1After = loadTable("exclude_db", "T1")
      Assertions.assertThat(lastSnapshotCommand(table1After).equals(CommitKind.COMPACT)).isTrue
      Assertions.assertThat(lastSnapshotId(table2)).isEqualTo(2)
    }

    spark.sql("DROP DATABASE IF EXISTS exclude_db CASCADE")
  }
}
