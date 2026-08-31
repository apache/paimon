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
import org.apache.paimon.partition.{Partition, PartitionStatistics}
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}

import java.util.Locale

import scala.collection.JavaConverters._

/**
 * `ANALYZE TABLE ... PARTITION(...) COMPUTE STATISTICS [NOSCAN]` on a Format Table with
 * catalog-managed partitions, held against the semantics Spark and Hive give the same statement on
 * a Hive metastore table.
 *
 * The reference behaviour is what Spark's own `StatisticsSuite` and its `AlterTable*Partition`
 * command suites pin: a partition column named without a value means every value of it, a spec
 * naming a partition that does not exist is an error rather than a no-op, and partition column
 * names resolve the way the rest of Spark resolves identifiers.
 */
class CatalogManagedPartitionAnalyzeTest extends PaimonSparkTestWithRestCatalogBase {

  test("ANALYZE with a full partition spec measures only that partition") {
    val tableName = "analyze_full_spec"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      writeCsvPartition(tableName, "20260101", "01", 2)
      repair(tableName)

      sql(
        s"ANALYZE TABLE ${qualified(tableName)} PARTITION (dt = '20260101', hour = '00') " +
          s"COMPUTE STATISTICS NOSCAN").collect()

      assert(statisticsOf(tableName, "20260101", "00").fileCount() == 1L)
      assert(!PartitionStatistics.isKnown(statisticsOf(tableName, "20260101", "01").fileCount()))
    }
  }

  test("ANALYZE with a leading prefix measures every partition under it") {
    val tableName = "analyze_prefix"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      writeCsvPartition(tableName, "20260101", "01", 2)
      writeCsvPartition(tableName, "20260102", "00", 3)
      repair(tableName)

      sql(
        s"ANALYZE TABLE ${qualified(tableName)} PARTITION (dt = '20260101') " +
          s"COMPUTE STATISTICS NOSCAN").collect()

      assert(statisticsOf(tableName, "20260101", "00").fileCount() == 1L)
      assert(statisticsOf(tableName, "20260101", "01").fileCount() == 1L)
      // The sibling day is outside the prefix and keeps whatever it had.
      assert(!PartitionStatistics.isKnown(statisticsOf(tableName, "20260102", "00").fileCount()))
    }
  }

  test("ANALYZE naming every partition column without a value measures every partition") {
    val tableName = "analyze_all_columns"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      writeCsvPartition(tableName, "20260102", "01", 2)
      repair(tableName)

      // Spark and Hive read `PARTITION (dt, hour)` as every value of both columns.
      sql(s"ANALYZE TABLE ${qualified(tableName)} PARTITION (dt, hour) COMPUTE STATISTICS NOSCAN")
        .collect()

      assert(statisticsOf(tableName, "20260101", "00").fileCount() == 1L)
      assert(statisticsOf(tableName, "20260102", "01").fileCount() == 1L)
    }
  }

  test("ANALYZE naming a trailing column without a value measures the set under the prefix") {
    val tableName = "analyze_partial_values"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      writeCsvPartition(tableName, "20260101", "01", 2)
      writeCsvPartition(tableName, "20260102", "00", 3)
      repair(tableName)

      // Spark and Hive read `PARTITION (dt = 'x', hour)` as every hour of that day.
      sql(
        s"ANALYZE TABLE ${qualified(tableName)} PARTITION (dt = '20260101', hour) " +
          s"COMPUTE STATISTICS NOSCAN").collect()

      assert(statisticsOf(tableName, "20260101", "00").fileCount() == 1L)
      assert(statisticsOf(tableName, "20260101", "01").fileCount() == 1L)
      assert(!PartitionStatistics.isKnown(statisticsOf(tableName, "20260102", "00").fileCount()))
    }
  }

  test("ANALYZE of a partition that does not exist fails instead of measuring nothing") {
    val tableName = "analyze_missing_partition"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      repair(tableName)

      // Succeeding silently tells the caller a partition was measured when none was.
      val error = intercept[Exception] {
        sql(
          s"ANALYZE TABLE ${qualified(tableName)} PARTITION (dt = '20991231') " +
            s"COMPUTE STATISTICS NOSCAN").collect()
      }
      // The same error Spark reports for a partition it cannot find on any other table.
      assert(causeMessages(error).contains("20991231"), causeMessages(error))
      assert(
        error.isInstanceOf[NoSuchPartitionException] ||
          error.getCause.isInstanceOf[NoSuchPartitionException],
        error)
    }
  }

  test("ANALYZE of a non-leading partition column is rejected instead of widened") {
    val tableName = "analyze_non_leading"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      writeCsvPartition(tableName, "20260102", "00", 2)
      repair(tableName)

      // The catalog selects on a leading run, so the only way to serve this spec is to measure
      // every day that has an hour 00 — more partitions than were asked about.
      val error = intercept[Exception] {
        sql(
          s"ANALYZE TABLE ${qualified(tableName)} PARTITION (hour = '00') " +
            s"COMPUTE STATISTICS NOSCAN").collect()
      }
      assert(
        causeMessages(error).contains("leading run of its partition columns [dt, hour]"),
        causeMessages(error))
      // Rejected means nothing was measured, in either of the two days the widening would reach.
      assert(!PartitionStatistics.isKnown(statisticsOf(tableName, "20260101", "00").fileCount()))
      assert(!PartitionStatistics.isKnown(statisticsOf(tableName, "20260102", "00").fileCount()))
    }
  }

  test("ANALYZE of a column that is not a partition column is rejected") {
    val tableName = "analyze_not_a_partition_column"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      repair(tableName)

      // `payload` is a column of the table but not one the catalog partitions on.
      val error = intercept[Exception] {
        sql(
          s"ANALYZE TABLE ${qualified(tableName)} PARTITION (payload = 'a') " +
            s"COMPUTE STATISTICS NOSCAN").collect()
      }
      assert(causeMessages(error).contains("payload"), causeMessages(error))
      // Dropping the name from the spec instead would measure the whole table.
      assert(!PartitionStatistics.isKnown(statisticsOf(tableName, "20260101", "00").fileCount()))
    }
  }

  test("ANALYZE resolves partition column names the way the rest of Spark resolves them") {
    val tableName = "analyze_case"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      repair(tableName)

      sql(
        s"ANALYZE TABLE ${qualified(tableName)} PARTITION (DT = '20260101') " +
          s"COMPUTE STATISTICS NOSCAN").collect()

      assert(statisticsOf(tableName, "20260101", "00").fileCount() == 1L)

      // And a case-sensitive session resolves it the way the rest of that session does.
      withSQLConf("spark.sql.caseSensitive" -> "true") {
        val error = intercept[Exception] {
          sql(
            s"ANALYZE TABLE ${qualified(tableName)} PARTITION (DT = '20260101') " +
              s"COMPUTE STATISTICS NOSCAN").collect()
        }
        assert(causeMessages(error).contains("DT"), causeMessages(error))
      }
    }
  }

  test("ANALYZE reads a partition value as the type of its partition column") {
    val tableName = "analyze_typed_partition"
    withTable(tableName) {
      sql(s"""CREATE TABLE $tableName (id INT, p INT)
             |USING CSV
             |PARTITIONED BY (p)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'true')
             |""".stripMargin)
      val table = formatTable(tableName)
      val partitionPath = new Path(table.location(), "p=1")
      table.fileIO().mkdirs(partitionPath)
      table.fileIO().writeFile(new Path(partitionPath, "part-00001.csv"), "1\n", false)
      repair(tableName)

      // The catalog holds the value the way Paimon writes it, so '01' names the partition it
      // registered as 1 rather than one that does not exist.
      sql(s"ANALYZE TABLE ${qualified(tableName)} PARTITION (p = '01') COMPUTE STATISTICS NOSCAN")
        .collect()

      assert(partitionOf(tableName, "p", "1").fileCount() == 1L)
    }
  }

  test("ANALYZE of a table with no registered partitions measures nothing") {
    val tableName = "analyze_no_registered_partitions"
    withTable(tableName) {
      createTable(tableName)
      // The other half of the guard that fails a PARTITION spec matching nothing: with no spec
      // there is nothing to have missed, so measuring nothing is the answer, not an error.
      writeCsvPartition(tableName, "20260101", "00", 1)

      sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS NOSCAN").collect()

      assert(registeredPartitions(tableName).isEmpty)
    }
  }

  test("ANALYZE ... FOR COLUMNS is rejected for a format table") {
    val tableName = "analyze_for_columns"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      repair(tableName)

      // A Format Table has nowhere to keep column statistics, so only the partition measurement
      // is intercepted and the column form keeps Spark's own rejection.
      intercept[Exception] {
        sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS FOR ALL COLUMNS").collect()
      }
      intercept[Exception] {
        sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS FOR COLUMNS payload")
          .collect()
      }
      assert(!PartitionStatistics.isKnown(statisticsOf(tableName, "20260101", "00").fileCount()))
    }
  }

  test("NOSCAN keeps a row count that is already known") {
    val tableName = "analyze_noscan_keeps"
    withTable(tableName) {
      // A full ANALYZE is the way this suite can put an exact row count in the catalog, so the
      // table is parquet; what the NOSCAN below must not do is erase it, however it was learned.
      sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
             |USING PARQUET
             |PARTITIONED BY (dt, hour)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'true')
             |""".stripMargin)
      sql(s"INSERT INTO ${qualified(tableName)} VALUES (1, 'a', '20260101', '00')")
      sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS").collect()
      val scanned = statisticsOf(tableName, "20260101", "00")
      assert(scanned.recordCount() == 1L, scanned.toString)

      // A listing cannot count rows, but it also learned nothing that contradicts the count that
      // is already there. Hive keeps numRows across a NOSCAN for exactly this reason.
      sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS NOSCAN").collect()

      val afterNoScan = statisticsOf(tableName, "20260101", "00")
      assert(afterNoScan.fileCount() == 1L, afterNoScan.toString)
      assert(afterNoScan.recordCount() == 1L, afterNoScan.toString)
    }
  }

  test("a full ANALYZE reads the row count a NOSCAN cannot") {
    val tableName = "analyze_footers"
    withTable(tableName) {
      // Parquet carries a row count in its footer. CSV, which the rest of this suite uses, carries
      // none, so it is the format that cannot tell a full ANALYZE apart from a NOSCAN.
      sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
             |USING PARQUET
             |PARTITIONED BY (dt, hour)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'true')
             |""".stripMargin)
      sql(s"""INSERT INTO ${qualified(tableName)}
             |VALUES (1, 'a', '20260101', '00'), (2, 'b', '20260101', '00')
             |""".stripMargin)
      // A commit reports the rows it wrote, so a partition it registered is already measured. The
      // two partitions below hold the same parquet files copied in from outside and registered by
      // a repair, which reports nothing, so their row count is nobody's measurement yet.
      copyPartitionFiles(tableName, "20260101", "20260102")
      copyPartitionFiles(tableName, "20260101", "20260103")
      repair(tableName)
      assert(!PartitionStatistics.isKnown(statisticsOf(tableName, "20260102", "00").recordCount()))

      sql(s"ANALYZE TABLE ${qualified(tableName)} PARTITION (dt = '20260102') COMPUTE STATISTICS")
        .collect()

      // No NOSCAN, so the footers were read and the row count is exact.
      val scanned = statisticsOf(tableName, "20260102", "00")
      assert(scanned.recordCount() == 2L, scanned.toString)
      assert(scanned.fileCount() >= 1L, scanned.toString)

      sql(
        s"ANALYZE TABLE ${qualified(tableName)} PARTITION (dt = '20260103') " +
          s"COMPUTE STATISTICS NOSCAN").collect()

      // NOSCAN measured from the listing alone: the file numbers are there, and the row count is
      // still nobody's measurement even though this format could have given one.
      val listed = statisticsOf(tableName, "20260103", "00")
      assert(listed.fileCount() >= 1L, listed.toString)
      assert(listed.fileSizeInBytes() > 0L, listed.toString)
      assert(!PartitionStatistics.isKnown(listed.recordCount()), listed.toString)
    }
  }

  test("SHOW TABLE EXTENDED displays catalog-managed Format Table partition statistics") {
    val tableName = "analyze_show_partition_statistics"
    withTable(tableName) {
      sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
             |USING PARQUET
             |PARTITIONED BY (dt, hour)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'true')
             |""".stripMargin)
      sql(s"""INSERT INTO ${qualified(tableName)} VALUES
             |(1, 'a', '20260101', '00'), (2, 'b', '20260101', '00')
             |""".stripMargin)
      sql(
        s"ANALYZE TABLE ${qualified(tableName)} " +
          s"PARTITION (dt = '20260101', hour = '00') COMPUTE STATISTICS").collect()

      val information =
        sql(
          s"SHOW TABLE EXTENDED IN paimon.$dbName0 LIKE '$tableName' " +
            s"PARTITION (dt = '20260101', hour = '00')")
          .select("information")
          .collect()
          .head
          .getString(0)

      assert(information.contains(s"${PartitionStatistics.FIELD_RECORD_COUNT}=2"), information)
      val statistics = statisticsOf(tableName, "20260101", "00")
      assert(statistics.fileCount() > 0L, statistics.toString)
      assert(statistics.fileSizeInBytes() > 0L, statistics.toString)
      assert(statistics.lastFileCreationTime() > 0L, statistics.toString)
      assert(
        information.contains(s"${PartitionStatistics.FIELD_FILE_COUNT}=${statistics.fileCount()}"),
        information)
      assert(
        information.contains(
          s"${PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES}=${statistics.fileSizeInBytes()}"),
        information)
      assert(
        information.contains(
          s"${PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME}=" +
            s"${statistics.lastFileCreationTime()}"),
        information)
      assert(
        information.matches("(?s).*Partition Statistics: 2 rows, [1-9]\\d* bytes.*"),
        information)
    }
  }

  test("a full ANALYZE clamps non-positive statistics parallelism") {
    Seq("zero" -> 0, "negative" -> -1).foreach {
      case (label, parallelism) =>
        val tableName = s"analyze_${label}_parallelism"
        withTable(tableName) {
          sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
                 |USING PARQUET
                 |PARTITIONED BY (dt, hour)
                 |TBLPROPERTIES (
                 |  'format-table.implementation' = 'paimon',
                 |  'metastore.partitioned-table' = 'true')
                 |""".stripMargin)
          sql(s"""INSERT INTO ${qualified(tableName)}
                 |VALUES (1, 'a', '20260101', '00'), (2, 'b', '20260101', '00')
                 |""".stripMargin)
          copyPartitionFiles(tableName, "20260101", "20260102")
          repair(tableName)
          assert(
            !PartitionStatistics.isKnown(statisticsOf(tableName, "20260102", "00").recordCount()))

          withSparkSQLConf(
            "spark.paimon.format-table.statistics.parallelism" -> parallelism.toString) {
            sql(
              s"ANALYZE TABLE ${qualified(tableName)} PARTITION (dt = '20260102') " +
                s"COMPUTE STATISTICS").collect()
          }

          val scanned = statisticsOf(tableName, "20260102", "00")
          // The exact row count confirms that ANALYZE completed the Parquet footer scan.
          assert(scanned.recordCount() == 2L, scanned.toString)
        }
    }
  }

  test("catalog partition row counts feed scan statistics after partition pruning") {
    val tableName = "analyze_scan_statistics"
    withTable(tableName) {
      createTable(tableName)
      sql(s"""INSERT INTO ${qualified(tableName)} VALUES
             |(1, 'a', '20260101', '00'),
             |(2, 'b', '20260101', '00'),
             |(3, 'c', '20260102', '00')
             |""".stripMargin)

      val all = getFormatTableScan(s"SELECT * FROM ${qualified(tableName)}")
      assert(all.estimateStatistics.numRows().getAsLong == 3L)

      val pruned =
        getFormatTableScan(s"SELECT * FROM ${qualified(tableName)} WHERE dt = '20260101'")
      assert(pruned.estimateStatistics.numRows().getAsLong == 2L)
    }
  }

  test("one unknown selected partition makes scan row count unknown") {
    val tableName = "analyze_partial_scan_statistics"
    withTable(tableName) {
      createTable(tableName)
      sql(
        s"INSERT INTO ${qualified(tableName)} VALUES " +
          s"(1, 'known', '20260101', '00')")
      writeCsvPartition(tableName, "20260102", "00", 2)
      repair(tableName)

      val known = getFormatTableScan(s"SELECT * FROM ${qualified(tableName)} WHERE dt = '20260101'")
      assert(known.estimateStatistics.numRows().getAsLong == 1L)

      val partiallyUnknown =
        getFormatTableScan(s"SELECT * FROM ${qualified(tableName)}").estimateStatistics
      assert(!partiallyUnknown.numRows().isPresent)
    }
  }

  test("a zero row count does not make a partition that still has files look free to read") {
    val tableName = "analyze_zero_row_count_size"
    withTable(tableName) {
      createTable(tableName)
      sql(
        s"INSERT INTO ${qualified(tableName)} VALUES " +
          s"(1, 'a', '20260101', '00'), (2, 'b', '20260101', '00')")

      // Positive control: a real measurement gives both a row count and a size.
      val measured = getFormatTableScan(s"SELECT * FROM ${qualified(tableName)}").estimateStatistics
      assert(measured.numRows().getAsLong == 2L)
      assert(measured.sizeInBytes().getAsLong > 0L)

      // Rewrite the statistics to zero without touching the files, the way a catalog that cannot
      // tell "never measured" from "measured, and empty" answers.
      val spec = Map("dt" -> "20260101", "hour" -> "00").asJava
      paimonCatalog.createPartitions(
        Identifier.create(dbName0, tableName),
        List(spec).asJava,
        true,
        List(
          new PartitionStatistics(
            spec,
            0L,
            0L,
            0L,
            0L,
            PartitionStatistics.UNKNOWN_TOTAL_BUCKETS)).asJava,
        true
      )

      val zeroed = getFormatTableScan(s"SELECT * FROM ${qualified(tableName)}").estimateStatistics
      // Neither number may say the scan is free: the size drives broadcast, the row count drives
      // join reordering.
      assert(!zeroed.numRows().isPresent, zeroed.numRows().toString)
      assert(zeroed.sizeInBytes().getAsLong > 0L, zeroed.sizeInBytes().toString)
    }
  }

  test("partition row count is not duplicated across format data splits") {
    val tableName = "analyze_multi_split_statistics"
    withTable(tableName) {
      sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
             |USING CSV
             |PARTITIONED BY (dt, hour)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'true',
             |  'source.split.target-size' = '1 B')
             |""".stripMargin)
      sql(s"""INSERT INTO ${qualified(tableName)} VALUES
             |(1, 'one', '20260101', '00'),
             |(2, 'two', '20260101', '00'),
             |(3, 'three', '20260101', '00')
             |""".stripMargin)

      val scan = getFormatTableScan(s"SELECT * FROM ${qualified(tableName)}")
      assert(scan.inputSplits.length > 1)
      assert(scan.inputSplits.forall(_.rowCount() == -1L))
      assert(scan.estimateStatistics.numRows().getAsLong == 3L)
    }
  }

  test("TPC-DS-style dimension join uses partition row count to avoid fact-side shuffle") {
    val tableName = "date_dim_format"
    withSparkSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.cbo.enabled" -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> "128",
      "spark.sql.join.preferSortMergeJoin" -> "true"
    ) {
      withTable(tableName) {
        sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
               |USING PARQUET
               |PARTITIONED BY (dt, hour)
               |TBLPROPERTIES (
               |  'format-table.implementation' = 'paimon',
               |  'metastore.partitioned-table' = 'true')
               |""".stripMargin)

        // Keep the physical file above the broadcast threshold while the projected dimension row
        // is tiny. The first partition is written normally, then copied as if an external writer
        // had added a new date partition without reporting its row count.
        val sparkSession = spark
        import sparkSession.implicits._
        val payload = new scala.util.Random(42L).alphanumeric.take(256 * 1024).mkString
        withTempView("date_dim_rows") {
          Seq((1, payload, "20260101", "00"))
            .toDF("id", "payload", "dt", "hour")
            .createOrReplaceTempView("date_dim_rows")
          sql(s"INSERT INTO ${qualified(tableName)} SELECT * FROM date_dim_rows")
        }
        copyPartitionFiles(tableName, "20260101", "20260102")
        repair(tableName)

        val query =
          s"""SELECT store_sales.id
             |FROM range(0, 1000000) store_sales
             |JOIN ${qualified(tableName)} date_dim
             |  ON store_sales.id = date_dim.id
             |WHERE date_dim.dt = '20260102' AND date_dim.hour = '00'
             |""".stripMargin

        val beforeStats = getFormatTableScan(query).estimateStatistics
        assert(!beforeStats.numRows().isPresent)
        val beforePlan = sql(query).queryExecution.executedPlan
        assert(
          beforePlan.collectFirst { case join: SortMergeJoinExec => join }.isDefined,
          beforePlan)
        assert(
          beforePlan.collectFirst { case join: BroadcastHashJoinExec => join }.isEmpty,
          beforePlan)

        sql(
          s"ANALYZE TABLE ${qualified(tableName)} " +
            s"PARTITION (dt = '20260102', hour = '00') COMPUTE STATISTICS").collect()

        val afterStats = getFormatTableScan(query).estimateStatistics
        assert(afterStats.numRows().getAsLong == 1L)
        val afterQuery = sql(query)
        val afterPlan = afterQuery.queryExecution.executedPlan
        val broadcastJoin = afterPlan
          .collectFirst { case join: BroadcastHashJoinExec => join }
          .getOrElse(fail(afterPlan.toString))
          .asInstanceOf[BroadcastHashJoinExec]
        assert(broadcastJoin.buildSide == BuildRight, afterPlan)
        checkAnswer(afterQuery, Seq(org.apache.spark.sql.Row(1L)))
      }
    }
  }

  test("ANALYZE run twice reports the same measurement rather than accumulating") {
    val tableName = "analyze_idempotent"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      repair(tableName)

      sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS NOSCAN").collect()
      val once = statisticsOf(tableName, "20260101", "00")
      sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS NOSCAN").collect()
      val twice = statisticsOf(tableName, "20260101", "00")

      // Anchored, so two runs that both measured nothing cannot pass as two equal measurements.
      assert(once.fileCount() == 1L, once.toString)
      assert(once.fileSizeInBytes() > 0L, once.toString)
      assert(twice.fileCount() == once.fileCount(), s"$once then $twice")
      assert(twice.fileSizeInBytes() == once.fileSizeInBytes(), s"$once then $twice")
    }
  }

  test("ANALYZE does not count files a committer left staged in the partition") {
    val tableName = "analyze_staging"
    withTable(tableName) {
      createTable(tableName)
      val partitionPath = writeCsvPartition(tableName, "20260101", "00", 1)
      val table = formatTable(tableName)
      // What a magic committer leaves behind: a data file name under a staging directory.
      val staged =
        new Path(new Path(partitionPath, "__magic_job-1/tasks/attempt_1/__base"), "part-9.csv")
      table.fileIO().mkdirs(staged.getParent)
      table.fileIO().writeFile(staged, "9,staged\n", false)
      repair(tableName)

      sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS NOSCAN").collect()

      val measured = statisticsOf(tableName, "20260101", "00")
      // The reader returns one row from one file; a measurement claiming two is a number no query
      // can reproduce.
      assert(measured.fileCount() == 1L, measured.toString)
      assert(sql(s"SELECT COUNT(*) FROM ${qualified(tableName)}").collect()(0).getLong(0) == 1L)
    }
  }

  test("ANALYZE measures registered partitions and never changes which exist") {
    val tableName = "analyze_partition_set"
    withTable(tableName) {
      createTable(tableName)
      writeCsvPartition(tableName, "20260101", "00", 1)
      writeCsvPartition(tableName, "20260102", "00", 2)
      repair(tableName)

      sql(
        s"ANALYZE TABLE ${qualified(tableName)} PARTITION (dt = '20260101') " +
          s"COMPUTE STATISTICS NOSCAN").collect()

      val scoped = statisticsOf(tableName, "20260101", "00")
      assert(scoped.fileCount() == 1L, scoped.toString)
      assert(scoped.fileSizeInBytes() > 0L, scoped.toString)
      // A PARTITION clause scopes the measurement; the sibling keeps whatever it had.
      val sibling = statisticsOf(tableName, "20260102", "00")
      assert(!PartitionStatistics.isKnown(sibling.fileCount()), sibling.toString)

      sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS NOSCAN").collect()
      assert(statisticsOf(tableName, "20260102", "00").fileCount() == 1L)
      // Analyzing measures partitions, it does not decide which ones exist.
      val expected = Set("dt=20260101/hour=00", "dt=20260102/hour=00")
      assert(registeredPartitions(tableName) == expected)
      assert(
        sql(s"SHOW PARTITIONS ${qualified(tableName)}").collect().map(_.getString(0)).toSet ==
          expected)
    }
  }

  test("ANALYZE is rejected for a format table discovering partitions from the filesystem") {
    val tableName = "analyze_filesystem_partitions"
    withTable(tableName) {
      sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
             |USING CSV
             |PARTITIONED BY (dt, hour)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'false')
             |""".stripMargin)

      // There is no catalog to write a measurement to, so the table is not intercepted at all and
      // keeps Spark's own rejection rather than quietly measuring nothing.
      val error = intercept[Exception] {
        sql(s"ANALYZE TABLE ${qualified(tableName)} COMPUTE STATISTICS NOSCAN").collect()
      }
      val messages = causeMessages(error)
      assert(messages.contains("ANALYZE TABLE"), messages)
      assert(messages.toLowerCase(Locale.ROOT).contains("not supported"), messages)
    }
  }

  private def qualified(tableName: String): String = s"paimon.$dbName0.$tableName"

  private def createTable(tableName: String): Unit = {
    sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING, hour STRING)
           |USING CSV
           |PARTITIONED BY (dt, hour)
           |TBLPROPERTIES (
           |  'format-table.implementation' = 'paimon',
           |  'metastore.partitioned-table' = 'true')
           |""".stripMargin)
  }

  private def repair(tableName: String): Unit =
    sql(s"MSCK REPAIR TABLE ${qualified(tableName)}").collect()

  private def formatTable(tableName: String): FormatTable =
    paimonCatalog.getTable(Identifier.create(dbName0, tableName)).asInstanceOf[FormatTable]

  private def writeCsvPartition(tableName: String, dt: String, hour: String, id: Int): Path = {
    val table = formatTable(tableName)
    val partitionPath = new Path(table.location(), s"dt=$dt/hour=$hour")
    table.fileIO().mkdirs(partitionPath)
    table
      .fileIO()
      .writeFile(new Path(partitionPath, f"part-$id%05d.csv"), s"$id,payload-$id\n", false)
    partitionPath
  }

  /** The same files under another partition value: written by nobody this catalog heard from. */
  private def copyPartitionFiles(tableName: String, sourceDt: String, targetDt: String): Unit = {
    val table = formatTable(tableName)
    val source = new Path(table.location(), s"dt=$sourceDt/hour=00")
    val target = new Path(table.location(), s"dt=$targetDt/hour=00")
    table.fileIO().mkdirs(target)
    table
      .fileIO()
      .listStatus(source)
      .filter(!_.isDir)
      .foreach(
        status =>
          table.fileIO().copyFile(status.getPath, new Path(target, status.getPath.getName), false))
  }

  private def registeredPartitions(tableName: String): Set[String] =
    paimonCatalog
      .listPartitions(Identifier.create(dbName0, tableName))
      .asScala
      .map(partition => s"dt=${partition.spec().get("dt")}/hour=${partition.spec().get("hour")}")
      .toSet

  private def statisticsOf(tableName: String, dt: String, hour: String): Partition =
    paimonCatalog
      .listPartitions(Identifier.create(dbName0, tableName))
      .asScala
      .find(p => p.spec().get("dt") == dt && p.spec().get("hour") == hour)
      .getOrElse(fail(s"partition dt=$dt/hour=$hour of $tableName is not registered"))

  private def partitionOf(tableName: String, key: String, value: String): Partition =
    paimonCatalog
      .listPartitions(Identifier.create(dbName0, tableName))
      .asScala
      .find(_.spec().get(key) == value)
      .getOrElse(fail(s"partition $key=$value of $tableName is not registered"))

  private def causeMessages(error: Throwable): String =
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .map(e => String.valueOf(e.getMessage))
      .mkString(" | ")
}
