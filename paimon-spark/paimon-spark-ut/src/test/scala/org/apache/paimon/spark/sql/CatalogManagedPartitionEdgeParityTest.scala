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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/**
 * The partition operations left over once ADD, DROP, SHOW and ANALYZE are covered: `TRUNCATE`,
 * `RENAME PARTITION`, and a null or empty string as a partition value. Spark pins all three for a
 * metastore table in `TruncateTableSuiteBase`, `AlterTableRenamePartitionSuiteBase` and the
 * `SPARK-33591` / `SPARK-33904` cases of the add, drop and show partition suites.
 *
 * An operation this table cannot support has to be refused outright. The failure worth catching is
 * a half-done one: the registration moved and the directory did not, or the data went away and the
 * registration stayed behind claiming it is still there.
 */
class CatalogManagedPartitionEdgeParityTest extends PaimonSparkTestWithRestCatalogBase {

  private val defaultPartitionName = "__DEFAULT_PARTITION__"

  // ------------------------------------------------------------------ TRUNCATE

  test("TRUNCATE TABLE either clears the data and keeps the partitions, or refuses outright") {
    val tableName = "edge_truncate_table"
    withTable(tableName) {
      createTable(tableName)
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', '20260101', '00')")
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (2, 'b', '20260102', '00')")
      val before = registered(tableName)

      val outcome = attempt(s"TRUNCATE TABLE ${qualified(tableName)}")

      outcome match {
        case Refused(_) =>
          // Refusing is a defensible answer; leaving the table half-truncated is not.
          assert(registered(tableName) == before)
          assert(rowIds(tableName) == Seq(1, 2))
        case Accepted =>
          // Spark keeps the partitions of a truncated table (SPARK-34418): truncation empties a
          // table, it does not redefine which partitions it has.
          assert(registered(tableName) == before, "TRUNCATE dropped partition registrations")
          assert(rowIds(tableName).isEmpty, "TRUNCATE left rows behind")
      }
    }
  }

  test("TRUNCATE TABLE PARTITION stays inside the partition it names, or refuses outright") {
    val tableName = "edge_truncate_partition"
    withTable(tableName) {
      createTable(tableName)
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', '20260101', '00')")
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (2, 'b', '20260102', '00')")
      val before = registered(tableName)

      val outcome =
        attempt(s"TRUNCATE TABLE ${qualified(tableName)} PARTITION (dt = '20260101', hour = '00')")

      outcome match {
        case Refused(message) =>
          assert(registered(tableName) == before)
          assert(rowIds(tableName) == Seq(1, 2))
          // The refusal has to name what is unsupported. This table has partitions and lists them,
          // so a message claiming otherwise sends the reader looking for the wrong problem.
          assert(
            !message.contains("Only FileStoreTable supports partitions"),
            s"refusal misdescribes the table: $message")
          assert(message.contains("MSCK REPAIR TABLE"), message)
        case Accepted =>
          assert(registered(tableName) == before, "TRUNCATE PARTITION dropped registrations")
          assert(rowIds(tableName) == Seq(2), "TRUNCATE PARTITION touched the wrong partitions")
      }
    }
  }

  // ------------------------------------------------------------------ RENAME PARTITION

  test("RENAME PARTITION moves both the registration and the data, or refuses outright") {
    val tableName = "edge_rename_partition"
    withTable(tableName) {
      createTable(tableName)
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', '20260101', '00')")
      val before = registered(tableName)

      val outcome = attempt(
        s"ALTER TABLE ${qualified(tableName)} PARTITION (dt = '20260101', hour = '00') " +
          s"RENAME TO PARTITION (dt = '20260201', hour = '00')")

      outcome match {
        case Refused(_) =>
          // Nothing may have moved: a registration pointing at a directory that is no longer
          // there, or data under a spec nobody registered, are both worse than a refusal.
          assert(registered(tableName) == before)
          assert(rowIds(tableName) == Seq(1))
          assert(directoryExists(tableName, "dt=20260101/hour=00"))
        case Accepted =>
          assert(registered(tableName) == Set("20260201/00"))
          assert(rowIds(tableName) == Seq(1), "renamed partition lost its rows")
          assert(directoryExists(tableName, "dt=20260201/hour=00"))
          assert(!directoryExists(tableName, "dt=20260101/hour=00"))
      }
    }
  }

  // ------------------------------------------------------------------ null / empty partition values

  test("a null partition value registers under the default partition name and reads back as null") {
    val tableName = "edge_null_partition"
    withTable(tableName) {
      createTable(tableName)

      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', NULL, '00')")

      assert(registered(tableName) == Set(s"$defaultPartitionName/00"))
      assert(directoryExists(tableName, s"dt=$defaultPartitionName/hour=00"))
      checkAnswer(
        sql(s"SELECT id, payload, dt, hour FROM ${qualified(tableName)}"),
        Seq(Row(1, "a", null, "00")))
    }
  }

  test("SHOW PARTITIONS spells a null partition value the way Spark's v2 path spells it") {
    val tableName = "edge_null_show"
    withTable(tableName) {
      createTable(tableName)
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', NULL, '00')")
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (2, 'b', '20260101', '00')")

      // Not what Hive prints. Spark's own ShowPartitionsExec renders a null partition value as the
      // literal "null" (`if (partValueUTF8String == null) "null"`), while its v1 path over a Hive
      // table prints __HIVE_DEFAULT_PARTITION__ because the metastore spec already holds that
      // string. Both the registration and the directory here use the default partition name, so
      // what SHOW prints is not a spec that can be pasted back into DROP PARTITION — see the
      // drop-by-name case below for the spelling that does work.
      val shown =
        sql(s"SHOW PARTITIONS ${qualified(tableName)}").collect().map(_.getString(0)).toSet
      assert(shown == Set("dt=null/hour=00", "dt=20260101/hour=00"), shown.mkString(", "))
      assert(registered(tableName).contains(s"$defaultPartitionName/00"))
    }
  }

  test("the null rendering is Spark's, not this table's: a native Paimon table prints it too") {
    val tableName = "edge_null_show_paimon"
    withTable(tableName) {
      sql(s"""CREATE TABLE $tableName (id INT, dt STRING)
             |PARTITIONED BY (dt)
             |""".stripMargin)
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, NULL)")

      val shown =
        sql(s"SHOW PARTITIONS ${qualified(tableName)}").collect().map(_.getString(0)).toSet
      // Same rendering on a table that has nothing to do with Format Tables, which puts the
      // divergence in Spark's v2 command rather than in the catalog-managed partition work.
      assert(shown == Set("dt=null"), shown.mkString(", "))
    }
  }

  test("MSCK discovers a default partition directory an outside writer left") {
    val tableName = "edge_null_msck"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, defaultPartitionName, "00", 7)

      sql(s"MSCK REPAIR TABLE ${qualified(tableName)}").collect()

      assert(registered(tableName) == Set(s"$defaultPartitionName/00"))
      // The registration is only worth something if the rows behind it are readable.
      assert(rowIds(tableName) == Seq(7))
    }
  }

  test("the default partition can be dropped by name and takes its data with it") {
    val tableName = "edge_null_drop"
    withTable(tableName) {
      createTable(tableName)
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', NULL, '00')")
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (2, 'b', '20260101', '00')")

      sql(
        s"ALTER TABLE ${qualified(tableName)} DROP PARTITION " +
          s"(dt = '$defaultPartitionName', hour = '00')")

      assert(registered(tableName) == Set("20260101/00"))
      assert(!directoryExists(tableName, s"dt=$defaultPartitionName/hour=00"))
      assert(rowIds(tableName) == Seq(2))
    }
  }

  test("ANALYZE measures the default partition like any other") {
    val tableName = "edge_null_analyze"
    withTable(tableName) {
      createTable(tableName)
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', NULL, '00')")

      sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS NOSCAN").collect()

      val measured = paimonCatalog
        .listPartitions(Identifier.create(dbName0, tableName))
        .asScala
        .head
      assert(measured.fileCount() == 1L, measured.toString)
      assert(measured.fileSizeInBytes() > 0L, measured.toString)
    }
  }

  test("an empty string partition value is not confused with a null one") {
    val tableName = "edge_empty_string"
    withTable(tableName) {
      createTable(tableName)

      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', '', '00')")

      // Whichever way an empty value is spelled on disk, the row has to read back the way it was
      // written, and the partition it landed in has to be the one that is registered.
      val registrations = registered(tableName)
      assert(registrations.size == 1, registrations.mkString(", "))
      val Array(dt, _) = registrations.head.split("/", 2)
      assert(directoryExists(tableName, s"dt=$dt/hour=00"), s"no directory for registered dt=$dt")
      assert(rowIds(tableName) == Seq(1))
    }
  }

  // ------------------------------------------------------------------ helpers

  sealed private trait Outcome
  private case object Accepted extends Outcome
  private case class Refused(message: String) extends Outcome

  /** Runs a statement, reporting whether it was accepted rather than failing the test outright. */
  private def attempt(statement: String): Outcome =
    try {
      sql(statement).collect()
      Accepted
    } catch {
      case error: Throwable =>
        val message = causeMessages(error)
        // scalastyle:off println
        println(s"[edge-parity] refused: $statement -> $message")
        // scalastyle:on println
        Refused(message)
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

  private def directoryExists(tableName: String, relativePath: String): Boolean = {
    val table = formatTable(tableName)
    table.fileIO().exists(new Path(table.location(), relativePath))
  }

  private def writeCsvPartition(tableName: String, dt: String, hour: String, id: Int): Unit = {
    val table = formatTable(tableName)
    val partitionPath = new Path(table.location(), s"dt=$dt/hour=$hour")
    table.fileIO().mkdirs(partitionPath)
    table
      .fileIO()
      .writeFile(new Path(partitionPath, f"part-$id%05d.csv"), s"$id,payload-$id\n", false)
  }

  private def registered(tableName: String): Set[String] =
    paimonCatalog
      .listPartitions(Identifier.create(dbName0, tableName))
      .asScala
      .map(p => s"${p.spec().get("dt")}/${p.spec().get("hour")}")
      .toSet

  private def rowIds(tableName: String): Seq[Int] =
    sql(s"SELECT id FROM ${qualified(tableName)} ORDER BY id").collect().map(_.getInt(0)).toSeq

  private def causeMessages(error: Throwable): String =
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .map(e => String.valueOf(e.getMessage))
      .mkString(" | ")
}
