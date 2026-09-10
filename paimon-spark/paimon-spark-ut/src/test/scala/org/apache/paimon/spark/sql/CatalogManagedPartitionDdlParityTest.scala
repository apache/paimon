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

import org.apache.paimon.CoreOptions
import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.{AnalysisException, Row}

import scala.collection.JavaConverters._

/**
 * Partition DDL on a Format Table with catalog-managed partitions, held against what Spark's own
 * `AlterTableAddPartitionSuite`, `AlterTableDropPartitionSuite` and `ShowPartitionsSuite` pin for a
 * metastore table: `IF NOT EXISTS` / `IF EXISTS` decide whether a repeat is an error, `SHOW
 * PARTITIONS` takes a partial spec and sorts its output, and partition column names resolve under
 * the session's case sensitivity.
 */
class CatalogManagedPartitionDdlParityTest extends PaimonSparkTestWithRestCatalogBase {

  test("ADD PARTITION LOCATION reads external data without creating the default directory") {
    val tableName = "ddl_add_location"
    withTable(tableName) {
      createTable(tableName)
      withTempDir {
        externalDir =>
          val table = formatTable(tableName)
          val externalLocation = new Path(externalDir.toURI.toString).toString
          table
            .fileIO()
            .writeFile(new Path(externalLocation, "part-00001.csv"), "1,a\n", false)

          sql(
            s"ALTER TABLE ${qualified(tableName)} ADD " +
              s"PARTITION (dt = '20260101', hour = '00') " +
              s"LOCATION '$externalLocation'")

          val partitions =
            paimonCatalog
              .listPartitions(Identifier.create(dbName0, tableName))
              .asScala
          assert(partitions.size == 1)
          assert(partitions.head.options().get(CoreOptions.PATH.key()) == externalLocation)
          assert(
            !table
              .fileIO()
              .exists(new Path(table.location(), "dt=20260101/hour=00")))
          checkAnswer(
            sql(
              s"SELECT id, payload, dt, hour FROM ${qualified(tableName)} " +
                s"WHERE dt = '20260101' AND hour = '00'"),
            Seq(Row(1, "a", "20260101", "00")))
      }
    }
  }

  test("ADD PARTITION IF NOT EXISTS is a repeatable no-op, a strict repeat is an error") {
    val tableName = "ddl_add_if_not_exists"
    withTable(tableName) {
      createTable(tableName)

      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260101', hour = '00')")
      sql(
        s"ALTER TABLE ${qualified(tableName)} ADD IF NOT EXISTS " +
          s"PARTITION (dt = '20260101', hour = '00')")
      assert(registered(tableName) == Set("20260101/00"))

      // Spark reports a duplicate as PartitionsAlreadyExistException; the point being pinned is
      // that a strict ADD does not quietly succeed.
      val error = intercept[Exception] {
        sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260101', hour = '00')")
      }
      assert(causeMessages(error).contains("20260101"), causeMessages(error))
      assert(registered(tableName) == Set("20260101/00"))
    }
  }

  test("DROP PARTITION IF EXISTS tolerates a partition that is not there") {
    val tableName = "ddl_drop_if_exists"
    withTable(tableName) {
      createTable(tableName)
      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260101', hour = '00')")

      sql(
        s"ALTER TABLE ${qualified(tableName)} DROP IF EXISTS " +
          s"PARTITION (dt = '20991231', hour = '00')")
      assert(registered(tableName) == Set("20260101/00"))

      sql(s"ALTER TABLE ${qualified(tableName)} DROP PARTITION (dt = '20260101', hour = '00')")
      assert(registered(tableName).isEmpty)
    }
  }

  test("ADD and DROP resolve partition column names the way the rest of Spark resolves them") {
    val tableName = "ddl_case_insensitive"
    withTable(tableName) {
      createTable(tableName)

      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (DT = '20260101', HOUR = '00')")
      assert(registered(tableName) == Set("20260101/00"))

      sql(s"ALTER TABLE ${qualified(tableName)} DROP PARTITION (Dt = '20260101', Hour = '00')")
      assert(registered(tableName).isEmpty)
    }
  }

  test("DROP with a leading prefix removes every partition under it and leaves the rest") {
    val tableName = "ddl_drop_prefix"
    withTable(tableName) {
      createTable(tableName)
      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260101', hour = '00')")
      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260101', hour = '01')")
      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260102', hour = '00')")

      sql(s"ALTER TABLE ${qualified(tableName)} DROP PARTITION (dt = '20260101')")

      assert(registered(tableName) == Set("20260102/00"))
    }
  }

  test("SHOW PARTITIONS takes a partial spec and returns sorted output") {
    val tableName = "ddl_show_partitions"
    withTable(tableName) {
      createTable(tableName)
      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260102', hour = '01')")
      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260101', hour = '01')")
      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260101', hour = '00')")

      val all = sql(s"SHOW PARTITIONS ${qualified(tableName)}").collect().map(_.getString(0)).toSeq
      assert(all == all.sorted, all.mkString(", "))
      assert(
        all == Seq("dt=20260101/hour=00", "dt=20260101/hour=01", "dt=20260102/hour=01"),
        all.mkString(", "))

      val scoped = sql(s"SHOW PARTITIONS ${qualified(tableName)} PARTITION (dt = '20260101')")
        .collect()
        .map(_.getString(0))
        .toSet
      assert(scoped == Set("dt=20260101/hour=00", "dt=20260101/hour=01"), scoped.mkString(", "))
    }
  }

  test("an INSERT registers the partition it wrote and the row reads back") {
    val tableName = "ddl_insert_registers"
    withTable(tableName) {
      createTable(tableName)

      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', '20260101', '00')")

      assert(registered(tableName) == Set("20260101/00"))
      checkAnswer(
        sql(s"SELECT id, payload, dt, hour FROM ${qualified(tableName)}"),
        Seq(Row(1, "a", "20260101", "00")))
    }
  }

  test("a registered partition whose directory is gone reads as empty rather than failing") {
    val tableName = "ddl_missing_directory"
    withTable(tableName) {
      createTable(tableName)
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', '20260101', '00')")
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (2, 'b', '20260102', '00')")

      val table = formatTable(tableName)
      table
        .fileIO()
        .deleteDirectoryQuietly(new org.apache.paimon.fs.Path(table.location(), "dt=20260101"))

      // The registration is the authority on which partitions exist; a missing directory is drift,
      // and drift reads as empty instead of taking the query down.
      checkAnswer(sql(s"SELECT id FROM ${qualified(tableName)} ORDER BY id"), Seq(Row(2)))
      assert(registered(tableName) == Set("20260101/00", "20260102/00"))
    }
  }

  test("SET LOCATION is rejected without changing a catalog-managed Format Table") {
    val tableName = "ddl_set_location"
    withTable(tableName) {
      createTable(tableName)
      val table = formatTable(tableName)
      val originalLocation = table.location().toString
      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260101', hour = '00')")
      table
        .fileIO()
        .writeFile(new Path(table.location(), "dt=20260101/hour=00/part-00001.csv"), "1,a\n", false)

      val error = intercept[UnsupportedOperationException] {
        sql(s"ALTER TABLE ${qualified(tableName)} SET LOCATION '${originalLocation}_relocated'")
      }

      assert(error.getMessage == "ALTER TABLE ... SET LOCATION is not supported for Paimon tables.")
      assert(formatTable(tableName).location().toString == originalLocation)
      assert(registered(tableName) == Set("20260101/00"))
      checkAnswer(
        sql(s"SELECT id, payload, dt, hour FROM ${qualified(tableName)}"),
        Seq(Row(1, "a", "20260101", "00")))
    }
  }

  test("partition SET LOCATION keeps Spark's structured rejection and table state") {
    val tableName = "ddl_partition_set_location"
    withTable(tableName) {
      createTable(tableName)
      val table = formatTable(tableName)
      val originalLocation = table.location().toString
      sql(s"ALTER TABLE ${qualified(tableName)} ADD PARTITION (dt = '20260101', hour = '00')")
      table
        .fileIO()
        .writeFile(new Path(table.location(), "dt=20260101/hour=00/part-00001.csv"), "1,a\n", false)

      val error = intercept[AnalysisException] {
        sql(
          s"ALTER TABLE ${qualified(tableName)} PARTITION " +
            s"(dt = '20260101', hour = '00') SET LOCATION '${originalLocation}_relocated'")
      }

      // Spark 4 renamed getErrorClass to getCondition and is still moving these legacy ids to
      // named conditions, so match the message instead of the id.
      assert(error.getMessage.contains("does not support partition"))
      assert(formatTable(tableName).location().toString == originalLocation)
      assert(registered(tableName) == Set("20260101/00"))
      checkAnswer(
        sql(s"SELECT id, payload, dt, hour FROM ${qualified(tableName)}"),
        Seq(Row(1, "a", "20260101", "00")))
    }
  }

  private def qualified(tableName: String): String = s"paimon.$dbName0.$tableName"

  private def createTable(tableName: String): Unit =
    sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
           |USING CSV
           |PARTITIONED BY (dt, hour)
           |TBLPROPERTIES (
           |  'format-table.implementation' = 'paimon',
           |  'metastore.partitioned-table' = 'true')
           |""".stripMargin)

  private def formatTable(tableName: String): FormatTable =
    paimonCatalog.getTable(Identifier.create(dbName0, tableName)).asInstanceOf[FormatTable]

  private def registered(tableName: String): Set[String] =
    paimonCatalog
      .listPartitions(Identifier.create(dbName0, tableName))
      .asScala
      .map(p => s"${p.spec().get("dt")}/${p.spec().get("hour")}")
      .toSet

  private def causeMessages(error: Throwable): String =
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .map(e => String.valueOf(e.getMessage))
      .mkString(" | ")
}
