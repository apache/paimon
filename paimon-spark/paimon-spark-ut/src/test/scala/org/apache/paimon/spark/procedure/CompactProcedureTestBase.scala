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
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.utils.SparkProcedureUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest
import org.assertj.core.api.Assertions
import org.scalatest.time.Span

import java.util

import scala.collection.JavaConverters._
import scala.util.Random

/** Test compact procedure. See [[CompactProcedure]]. */
abstract class CompactProcedureTestBase extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  // ----------------------- Minor Compact -----------------------

  test("Paimon Procedure: compact aware bucket pk table with minor compact strategy") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING, pt STRING)
                   |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='1', 'write-only'='true')
                   |PARTITIONED BY (pt)
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
      spark.sql(s"INSERT INTO T VALUES (3, 'c', 'p1'), (4, 'd', 'p2')")

      Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.APPEND)).isTrue
      Assertions.assertThat(lastSnapshotId(table)).isEqualTo(2)

      spark.sql(
        "CALL sys.compact(table => 'T', compact_strategy => 'minor'," +
          "options => 'num-sorted-run.compaction-trigger=3')")

      // Due to the limitation of parameter 'num-sorted-run.compaction-trigger' = 3, so compact is not
      // performed.
      Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.APPEND)).isTrue
      Assertions.assertThat(lastSnapshotId(table)).isEqualTo(2)

      // Make par-p1 has 3 datafile and par-p2 has 2 datafile, so par-p2 will not be picked out to
      // compact.
      spark.sql(s"INSERT INTO T VALUES (1, 'a', 'p1')")

      spark.sql(
        "CALL sys.compact(table => 'T', compact_strategy => 'minor'," +
          "options => 'num-sorted-run.compaction-trigger=3')")

      Assertions.assertThat(lastSnapshotId(table)).isEqualTo(4)
      Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue

      val splits = table.newSnapshotReader.read.dataSplits
      splits.forEach(
        split => {
          Assertions
            .assertThat(split.dataFiles.size)
            .isEqualTo(if (split.partition().getString(0).toString == "p2") 2 else 1)
        })
    }
  }

  // ----------------------- Sort Compact -----------------------

  test("Paimon Procedure: sort compact") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b INT)
                       |TBLPROPERTIES ('bucket'='-1')
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int)]
          val stream = inputData
            .toDS()
            .toDF("a", "b")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // test zorder sort
            inputData.addData((0, 0))
            inputData.addData((0, 1))
            inputData.addData((0, 2))
            inputData.addData((1, 0))
            inputData.addData((1, 1))
            inputData.addData((1, 2))
            inputData.addData((2, 0))
            inputData.addData((2, 1))
            inputData.addData((2, 2))
            stream.processAllAvailable()

            val result = new util.ArrayList[Row]()
            for (a <- 0 until 3) {
              for (b <- 0 until 3) {
                result.add(Row(a, b))
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)

            checkAnswer(
              spark.sql(
                "CALL paimon.sys.compact(table => 'T', order_strategy => 'zorder', order_by => 'a,b')"),
              Row(true) :: Nil)

            val result2 = new util.ArrayList[Row]()
            result2.add(0, Row(0, 0))
            result2.add(1, Row(0, 1))
            result2.add(2, Row(1, 0))
            result2.add(3, Row(1, 1))
            result2.add(4, Row(0, 2))
            result2.add(5, Row(1, 2))
            result2.add(6, Row(2, 0))
            result2.add(7, Row(2, 1))
            result2.add(8, Row(2, 2))

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result2)

            // test hilbert sort
            val result3 = new util.ArrayList[Row]()
            result3.add(0, Row(0, 0))
            result3.add(1, Row(0, 1))
            result3.add(2, Row(1, 1))
            result3.add(3, Row(1, 0))
            result3.add(4, Row(2, 0))
            result3.add(5, Row(2, 1))
            result3.add(6, Row(2, 2))
            result3.add(7, Row(1, 2))
            result3.add(8, Row(0, 2))

            checkAnswer(
              spark.sql(
                "CALL paimon.sys.compact(table => 'T', order_strategy => 'hilbert', order_by => 'a,b')"),
              Row(true) :: Nil)

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result3)

            // test order sort
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.compact(table => 'T', order_strategy => 'order', order_by => 'a,b')"),
              Row(true) :: Nil)
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: sort compact with partition") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (p INT, a INT, b INT)
                       |TBLPROPERTIES ('bucket'='-1')
                       |PARTITIONED BY (p)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int, Int)]
          val stream = inputData
            .toDS()
            .toDF("p", "a", "b")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query0 = () => spark.sql("SELECT * FROM T WHERE p=0")
          val query1 = () => spark.sql("SELECT * FROM T WHERE p=1")

          try {
            // test zorder sort
            inputData.addData((0, 0, 0))
            inputData.addData((0, 0, 1))
            inputData.addData((0, 0, 2))
            inputData.addData((0, 1, 0))
            inputData.addData((0, 1, 1))
            inputData.addData((0, 1, 2))
            inputData.addData((0, 2, 0))
            inputData.addData((0, 2, 1))
            inputData.addData((0, 2, 2))

            inputData.addData((1, 0, 0))
            inputData.addData((1, 0, 1))
            inputData.addData((1, 0, 2))
            inputData.addData((1, 1, 0))
            inputData.addData((1, 1, 1))
            inputData.addData((1, 1, 2))
            inputData.addData((1, 2, 0))
            inputData.addData((1, 2, 1))
            inputData.addData((1, 2, 2))
            stream.processAllAvailable()

            val result0 = new util.ArrayList[Row]()
            for (a <- 0 until 3) {
              for (b <- 0 until 3) {
                result0.add(Row(0, a, b))
              }
            }
            val result1 = new util.ArrayList[Row]()
            for (a <- 0 until 3) {
              for (b <- 0 until 3) {
                result1.add(Row(1, a, b))
              }
            }
            Assertions.assertThat(query0().collect()).containsExactlyElementsOf(result0)
            Assertions.assertThat(query1().collect()).containsExactlyElementsOf(result1)

            checkAnswer(
              spark.sql(
                "CALL paimon.sys.compact(table => 'T', partitions => 'p=0',  order_strategy => 'zorder', order_by => 'a,b')"),
              Row(true) :: Nil)

            val result2 = new util.ArrayList[Row]()
            result2.add(0, Row(0, 0, 0))
            result2.add(1, Row(0, 0, 1))
            result2.add(2, Row(0, 1, 0))
            result2.add(3, Row(0, 1, 1))
            result2.add(4, Row(0, 0, 2))
            result2.add(5, Row(0, 1, 2))
            result2.add(6, Row(0, 2, 0))
            result2.add(7, Row(0, 2, 1))
            result2.add(8, Row(0, 2, 2))

            Assertions.assertThat(query0().collect()).containsExactlyElementsOf(result2)
            Assertions.assertThat(query1().collect()).containsExactlyElementsOf(result1)

            // test hilbert sort
            val result3 = new util.ArrayList[Row]()
            result3.add(0, Row(0, 0, 0))
            result3.add(1, Row(0, 0, 1))
            result3.add(2, Row(0, 1, 1))
            result3.add(3, Row(0, 1, 0))
            result3.add(4, Row(0, 2, 0))
            result3.add(5, Row(0, 2, 1))
            result3.add(6, Row(0, 2, 2))
            result3.add(7, Row(0, 1, 2))
            result3.add(8, Row(0, 0, 2))

            checkAnswer(
              spark.sql(
                "CALL paimon.sys.compact(table => 'T', partitions => 'p=0',  order_strategy => 'hilbert', order_by => 'a,b')"),
              Row(true) :: Nil)

            Assertions.assertThat(query0().collect()).containsExactlyElementsOf(result3)
            Assertions.assertThat(query1().collect()).containsExactlyElementsOf(result1)

            // test order sort
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.compact(table => 'T', partitions => 'p=0',  order_strategy => 'order', order_by => 'a,b')"),
              Row(true) :: Nil)
            Assertions.assertThat(query0().collect()).containsExactlyElementsOf(result0)
            Assertions.assertThat(query1().collect()).containsExactlyElementsOf(result1)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: sort compact with multi-partitions") {
    Seq("order", "zorder").foreach {
      orderStrategy =>
        {
          withTable("T") {
            spark.sql(s"""
                         |CREATE TABLE T (id INT, pt STRING)
                         |PARTITIONED BY (pt)
                         |""".stripMargin)

            spark.sql(s"""INSERT INTO T VALUES
                         |(1, 'p1'), (3, 'p1'),
                         |(1, 'p2'), (4, 'p2'),
                         |(3, 'p3'), (2, 'p3'),
                         |(1, 'p4'), (2, 'p4')
                         |""".stripMargin)

            spark.sql(s"""INSERT INTO T VALUES
                         |(4, 'p1'), (2, 'p1'),
                         |(2, 'p2'), (3, 'p2'),
                         |(1, 'p3'), (4, 'p3'),
                         |(3, 'p4'), (4, 'p4')
                         |""".stripMargin)

            checkAnswer(
              spark.sql(
                s"CALL sys.compact(table => 'T', order_strategy => '$orderStrategy', order_by => 'id')"),
              Seq(true).toDF())

            val result = List(Row(1), Row(2), Row(3), Row(4)).asJava
            Seq("p1", "p2", "p3", "p4").foreach {
              pt =>
                Assertions
                  .assertThat(spark.sql(s"SELECT id FROM T WHERE pt='$pt'").collect())
                  .containsExactlyElementsOf(result)
            }
          }
        }
    }
  }

  test("Paimon Procedure: sort compact with partition filter") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, pt INT) PARTITIONED BY (pt)")
      sql("INSERT INTO t VALUES (1, 1)")
      sql("INSERT INTO t VALUES (2, 1)")
      sql(
        "CALL sys.compact(table => 't', order_strategy => 'order', where => 'pt = 1', order_by => 'a')")
      val table = loadTable("t")
      assert(table.latestSnapshot().get().commitKind.equals(CommitKind.OVERWRITE))
      checkAnswer(sql("SELECT * FROM t ORDER BY a"), Seq(Row(1, 1), Row(2, 1)))
    }
  }

  test("Paimon Procedure: compact for pk") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (a INT, b INT)
                       |TBLPROPERTIES ('primary-key'='a,b', 'bucket'='1')
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int)]
          val stream = inputData
            .toDS()
            .toDF("a", "b")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            inputData.addData((0, 0))
            inputData.addData((0, 1))
            inputData.addData((0, 2))
            inputData.addData((1, 0))
            inputData.addData((1, 1))
            inputData.addData((1, 2))
            inputData.addData((2, 0))
            inputData.addData((2, 1))
            inputData.addData((2, 2))
            stream.processAllAvailable()

            val result = new util.ArrayList[Row]()
            for (a <- 0 until 3) {
              for (b <- 0 until 3) {
                result.add(Row(a, b))
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)
            checkAnswer(spark.sql("CALL paimon.sys.compact(table => 'T')"), Row(true) :: Nil)
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: compact aware bucket pk table") {
    Seq(1, -1).foreach(
      bucket => {
        withTable("T") {
          spark.sql(
            s"""
               |CREATE TABLE T (id INT, value STRING, pt STRING)
               |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='$bucket', 'write-only'='true')
               |PARTITIONED BY (pt)
               |""".stripMargin)

          val table = loadTable("T")

          spark.sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
          spark.sql(s"INSERT INTO T VALUES (3, 'c', 'p1'), (4, 'd', 'p2')")

          spark.sql("CALL sys.compact(table => 'T', partitions => 'pt=\"p1\"')")
          Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
          Assertions.assertThat(lastSnapshotId(table)).isEqualTo(3)

          spark.sql(s"CALL sys.compact(table => 'T')")
          Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
          Assertions.assertThat(lastSnapshotId(table)).isEqualTo(4)

          // compact condition no longer met
          spark.sql(s"CALL sys.compact(table => 'T')")
          Assertions.assertThat(lastSnapshotId(table)).isEqualTo(4)

          checkAnswer(
            spark.sql(s"SELECT * FROM T ORDER BY id"),
            Row(1, "a", "p1") :: Row(2, "b", "p2") :: Row(3, "c", "p1") :: Row(4, "d", "p2") :: Nil)
        }
      })
  }

  test("Paimon Procedure: compact aware bucket pk table with many small files") {
    Seq(3, -1).foreach(
      bucket => {
        withTable("T") {
          spark.sql(
            s"""
               |CREATE TABLE T (id INT, value STRING, pt STRING)
               |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='$bucket', 'write-only'='true',
               |'source.split.target-size'='128m','source.split.open-file-cost'='32m') -- simulate multiple splits in a single bucket
               |PARTITIONED BY (pt)
               |""".stripMargin)

          val table = loadTable("T")

          val count = 100
          for (i <- 0 until count) {
            spark.sql(s"INSERT INTO T VALUES ($i, 'a', 'p${i % 2}')")
          }

          spark.sql(s"CALL sys.compact(table => 'T')")
          Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
          checkAnswer(spark.sql(s"SELECT COUNT(*) FROM T"), Row(count) :: Nil)
        }
      })
  }

  test("Paimon Procedure: compact unaware bucket append table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING, pt STRING)
                 |TBLPROPERTIES ('bucket'='-1', 'write-only'='true', 'compaction.min.file-num'='2')
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    val table = loadTable("T")

    spark.sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
    spark.sql(s"INSERT INTO T VALUES (3, 'c', 'p1'), (4, 'd', 'p2')")
    spark.sql(s"INSERT INTO T VALUES (5, 'e', 'p1'), (6, 'f', 'p2')")

    spark.sql("CALL sys.compact(table => 'T', partitions => 'pt=\"p1\"')")
    Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
    Assertions.assertThat(lastSnapshotId(table)).isEqualTo(4)

    spark.sql(s"CALL sys.compact(table => 'T')")
    Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
    Assertions.assertThat(lastSnapshotId(table)).isEqualTo(5)

    // compact condition no longer met
    spark.sql(s"CALL sys.compact(table => 'T')")
    Assertions.assertThat(lastSnapshotId(table)).isEqualTo(5)

    checkAnswer(
      spark.sql(s"SELECT * FROM T ORDER BY id"),
      Row(1, "a", "p1") :: Row(2, "b", "p2") :: Row(3, "c", "p1") :: Row(4, "d", "p2") :: Row(
        5,
        "e",
        "p1") :: Row(6, "f", "p2") :: Nil)
  }

  test("Paimon Procedure: compact unaware bucket append table with many small files") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING, pt STRING)
                 |TBLPROPERTIES ('bucket'='-1', 'write-only'='true')
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    val table = loadTable("T")

    val count = 100
    for (i <- 0 until count) {
      spark.sql(s"INSERT INTO T VALUES ($i, 'a', 'p${i % 2}')")
    }

    spark.sql(s"CALL sys.compact(table => 'T')")
    Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
    checkAnswer(spark.sql(s"SELECT COUNT(*) FROM T"), Row(count) :: Nil)
  }

  test("Paimon Procedure: compact with wrong usage") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING, pt STRING)
                 |TBLPROPERTIES ('bucket'='-1', 'write-only'='true')
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    assert(intercept[IllegalArgumentException] {
      spark.sql(
        "CALL sys.compact(table => 'T', partitions => 'pt = \"p1\"', where => 'pt = \"p1\"')")
    }.getMessage.contains("partitions and where cannot be used together"))

    assert(intercept[IllegalArgumentException] {
      spark.sql("CALL sys.compact(table => 'T', partitions => 'id = 1')")
    }.getMessage.contains("Only partition predicate is supported"))

    assert(intercept[IllegalArgumentException] {
      spark.sql("CALL sys.compact(table => 'T', where => 'id > 1 AND pt = \"p1\"')")
    }.getMessage.contains("Only partition predicate is supported"))

    assert(intercept[IllegalArgumentException] {
      spark.sql("CALL sys.compact(table => 'T', order_strategy => 'sort', order_by => 'pt')")
    }.getMessage.contains("order_by should not contain partition cols"))

    assert(intercept[IllegalArgumentException] {
      spark.sql(
        "CALL sys.compact(table => 'T', order_strategy => 'sort', order_by => 'id', partition_idle_time =>'5s')")
    }.getMessage.contains("sort compact do not support 'partition_idle_time'"))
  }

  test("Paimon Procedure: compact with where") {
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, value STRING, dt STRING, hh INT)
         |TBLPROPERTIES ('bucket'='1', 'bucket-key'='id', 'write-only'='true', 'compaction.min.file-num'='1')
         |PARTITIONED BY (dt, hh)
         |""".stripMargin)

    val table = loadTable("T")
    val fileIO = table.fileIO()

    spark.sql(s"INSERT INTO T VALUES (1, '1', '2024-01-01', 0), (2, '2', '2024-01-01', 1)")
    spark.sql(s"INSERT INTO T VALUES (3, '3', '2024-01-01', 0), (4, '4', '2024-01-01', 1)")
    spark.sql(s"INSERT INTO T VALUES (5, '5', '2024-01-02', 0), (6, '6', '2024-01-02', 1)")
    spark.sql(s"INSERT INTO T VALUES (7, '7', '2024-01-02', 0), (8, '8', '2024-01-02', 1)")

    spark.sql("CALL sys.compact(table => 'T', where => 'dt = \"2024-01-01\" and hh >= 1')")
    Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
    Assertions
      .assertThat(
        fileIO.listStatus(new Path(table.location(), "dt=2024-01-01/hh=0/bucket-0")).length)
      .isEqualTo(2)
    Assertions
      .assertThat(
        fileIO.listStatus(new Path(table.location(), "dt=2024-01-01/hh=1/bucket-0")).length)
      .isEqualTo(3)
    Assertions
      .assertThat(
        fileIO.listStatus(new Path(table.location(), "dt=2024-01-02/hh=0/bucket-0")).length)
      .isEqualTo(2)
    Assertions
      .assertThat(
        fileIO.listStatus(new Path(table.location(), "dt=2024-01-02/hh=1/bucket-0")).length)
      .isEqualTo(2)
  }

  test("Paimon test: toWhere method in CompactProcedure") {
    val conditions = "f0=0,f1=0,f2=0;f0=1,f1=1,f2=1;f0=1,f1=2,f2=2;f3=3"

    val where = SparkProcedureUtils.toWhere(conditions)
    val whereExpected =
      "(f0=0 AND f1=0 AND f2=0) OR (f0=1 AND f1=1 AND f2=1) OR (f0=1 AND f1=2 AND f2=2) OR (f3=3)"

    Assertions.assertThat(where).isEqualTo(whereExpected)
  }

  test("Paimon Procedure: compact unaware bucket append table with option") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING, pt STRING)
                 |TBLPROPERTIES ('bucket'='-1', 'write-only'='true')
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    val table = loadTable("T")

    spark.sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
    spark.sql(s"INSERT INTO T VALUES (3, 'c', 'p1'), (4, 'd', 'p2')")
    spark.sql(s"INSERT INTO T VALUES (5, 'e', 'p1'), (6, 'f', 'p2')")

    spark.sql(
      "CALL sys.compact(table => 'T', partitions => 'pt=\"p1\"', options => 'compaction.min.file-num=2')")
    Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
    Assertions.assertThat(lastSnapshotId(table)).isEqualTo(4)

    spark.sql("CALL sys.compact(table => 'T', options => 'compaction.min.file-num=2')")
    Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
    Assertions.assertThat(lastSnapshotId(table)).isEqualTo(5)

    // compact condition no longer met
    spark.sql(s"CALL sys.compact(table => 'T')")
    Assertions.assertThat(lastSnapshotId(table)).isEqualTo(5)

    checkAnswer(
      spark.sql(s"SELECT * FROM T ORDER BY id"),
      Row(1, "a", "p1") :: Row(2, "b", "p2") :: Row(3, "c", "p1") :: Row(4, "d", "p2") ::
        Row(5, "e", "p1") :: Row(6, "f", "p2") :: Nil)
  }

  test("Paimon Procedure: compact with partition_idle_time for pk table") {
    Seq(1, -1).foreach(
      bucket => {
        withTable("T") {
          val dynamicBucketArgs = if (bucket == -1) " ,'dynamic-bucket.initial-buckets'='1'" else ""
          spark.sql(
            s"""
               |CREATE TABLE T (id INT, value STRING, dt STRING, hh INT)
               |TBLPROPERTIES ('primary-key'='id, dt, hh', 'bucket'='$bucket', 'write-only'='true'$dynamicBucketArgs)
               |PARTITIONED BY (dt, hh)
               |""".stripMargin)

          val table = loadTable("T")

          spark.sql(s"INSERT INTO T VALUES (1, '1', '2024-01-01', 0), (2, '2', '2024-01-01', 1)")
          spark.sql(s"INSERT INTO T VALUES (5, '5', '2024-01-02', 0), (6, '6', '2024-01-02', 1)")
          spark.sql(s"INSERT INTO T VALUES (3, '3', '2024-01-01', 0), (4, '4', '2024-01-01', 1)")
          spark.sql(s"INSERT INTO T VALUES (7, '7', '2024-01-02', 0), (8, '8', '2024-01-02', 1)")

          Thread.sleep(10000);
          spark.sql(s"INSERT INTO T VALUES (9, '9', '2024-01-01', 0), (10, '10', '2024-01-02', 0)")

          spark.sql("CALL sys.compact(table => 'T', partition_idle_time => '10s')")
          val dataSplits = table.newSnapshotReader.read.dataSplits.asScala.toList
          Assertions
            .assertThat(dataSplits.size)
            .isEqualTo(4)
          Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
          for (dataSplit: DataSplit <- dataSplits) {
            if (dataSplit.partition().getInt(1) == 0) {
              Assertions
                .assertThat(dataSplit.dataFiles().size())
                .isEqualTo(3)
            } else {
              Assertions
                .assertThat(dataSplit.dataFiles().size())
                .isEqualTo(1)
            }
          }
        }
      })

  }

  test("Paimon Procedure: compact with partition_idle_time for unaware bucket append table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING, dt STRING, hh INT)
                 |TBLPROPERTIES ('bucket'='-1', 'write-only'='true', 'compaction.min.file-num'='2')
                 |PARTITIONED BY (dt, hh)
                 |""".stripMargin)

    val table = loadTable("T")

    spark.sql(s"INSERT INTO T VALUES (1, '1', '2024-01-01', 0), (2, '2', '2024-01-01', 1)")
    spark.sql(s"INSERT INTO T VALUES (5, '5', '2024-01-02', 0), (6, '6', '2024-01-02', 1)")
    spark.sql(s"INSERT INTO T VALUES (3, '3', '2024-01-01', 0), (4, '4', '2024-01-01', 1)")
    spark.sql(s"INSERT INTO T VALUES (7, '7', '2024-01-02', 0), (8, '8', '2024-01-02', 1)")

    Thread.sleep(10000);
    spark.sql(s"INSERT INTO T VALUES (9, '9', '2024-01-01', 0), (10, '10', '2024-01-02', 0)")

    spark.sql("CALL sys.compact(table => 'T', partition_idle_time => '10s')")
    val dataSplits = table.newSnapshotReader.read.dataSplits.asScala.toList
    Assertions
      .assertThat(dataSplits.size)
      .isEqualTo(4)
    Assertions.assertThat(lastSnapshotCommand(table).equals(CommitKind.COMPACT)).isTrue
    for (dataSplit: DataSplit <- dataSplits) {
      if (dataSplit.partition().getInt(1) == 0) {
        Assertions
          .assertThat(dataSplit.dataFiles().size())
          .isEqualTo(3)
      } else {
        Assertions
          .assertThat(dataSplit.dataFiles().size())
          .isEqualTo(1)
      }
    }
  }

  test("Paimon Procedure: test aware-bucket compaction read parallelism") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING)
                 |TBLPROPERTIES ('primary-key'='id', 'bucket'='3', 'write-only'='true')
                 |""".stripMargin)

    val table = loadTable("T")
    for (i <- 1 to 10) {
      sql(s"INSERT INTO T VALUES ($i, '$i')")
    }
    assertResult(10)(table.snapshotManager().snapshotCount())

    val buckets = table.newSnapshotReader().bucketEntries().asScala.map(_.bucket()).distinct.size
    assertResult(3)(buckets)

    val taskBuffer = scala.collection.mutable.ListBuffer.empty[Int]
    val listener = new SparkListener {
      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        taskBuffer += stageSubmitted.stageInfo.numTasks
      }
    }

    try {
      spark.sparkContext.addSparkListener(listener)

      // spark.default.parallelism cannot be change in spark session
      // sparkParallelism is 2, bucket is 3, use 2 as the read parallelism
      spark.conf.set("spark.sql.shuffle.partitions", 2)
      spark.sql("CALL sys.compact(table => 'T')")

      // sparkParallelism is 5, bucket is 3, use 3 as the read parallelism
      spark.conf.set("spark.sql.shuffle.partitions", 5)
      spark.sql("CALL sys.compact(table => 'T')")

      assertResult(Seq(2, 3))(taskBuffer)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("Paimon Procedure: test unaware-bucket compaction read parallelism") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING)
                 |TBLPROPERTIES ('bucket'='-1', 'write-only'='true')
                 |""".stripMargin)

    val table = loadTable("T")
    for (i <- 1 to 12) {
      sql(s"INSERT INTO T VALUES ($i, '$i')")
    }
    assertResult(12)(table.snapshotManager().snapshotCount())

    val buckets = table.newSnapshotReader().bucketEntries().asScala.map(_.bucket()).distinct.size
    // only has bucket-0
    assertResult(1)(buckets)

    val taskBuffer = scala.collection.mutable.ListBuffer.empty[Int]
    val listener = new SparkListener {
      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        taskBuffer += stageSubmitted.stageInfo.numTasks
      }
    }

    try {
      spark.sparkContext.addSparkListener(listener)

      // spark.default.parallelism cannot be change in spark session
      // sparkParallelism is 2, task groups is 6, use 2 as the read parallelism
      spark.conf.set("spark.sql.shuffle.partitions", 2)
      spark.sql(
        "CALL sys.compact(table => 'T', options => 'source.split.open-file-cost=3200M, compaction.min.file-num=2')")

      // sparkParallelism is 5, task groups is 1, use 1 as the read parallelism
      spark.conf.set("spark.sql.shuffle.partitions", 5)
      spark.sql(
        "CALL sys.compact(table => 'T', options => 'source.split.open-file-cost=3200M, compaction.min.file-num=2')")

      assertResult(Seq(2, 3))(taskBuffer)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("Paimon Procedure: type cast in where") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, value STRING, day_part LONG)
            |TBLPROPERTIES ('compaction.min.file-num'='2')
            |PARTITIONED BY (day_part)
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a', 20250810)")
      sql("INSERT INTO t VALUES (2, 'b', 20250810)")
      sql("INSERT INTO t VALUES (3, 'c', 20250811)")

      sql("CALL sys.compact(table => 't', where => 'day_part < 20250811 and day_part > 20250809')")
      val table = loadTable("t")
      assert(table.snapshotManager().latestSnapshot().commitKind().equals(CommitKind.COMPACT))
    }
  }

  test("Paimon Procedure: cluster for unpartitioned table") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b INT, c STRING)
               |TBLPROPERTIES ('bucket'='-1','num-levels'='6', 'num-sorted-run.compaction-trigger'='2', 'clustering.columns'='a,b', 'clustering.strategy'='zorder', 'clustering.incremental' = 'true')
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int, String)]
          val stream = inputData
            .toDS()
            .toDF("a", "b", "c")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            val random = new Random()
            val randomStr = random.nextString(40)
            // first write
            inputData.addData((0, 0, randomStr))
            inputData.addData((0, 1, randomStr))
            inputData.addData((0, 2, randomStr))
            inputData.addData((1, 0, randomStr))
            inputData.addData((1, 1, randomStr))
            inputData.addData((1, 2, randomStr))
            inputData.addData((2, 0, randomStr))
            inputData.addData((2, 1, randomStr))
            inputData.addData((2, 2, randomStr))
            stream.processAllAvailable()

            val result = new util.ArrayList[Row]()
            for (a <- 0 until 3) {
              for (b <- 0 until 3) {
                result.add(Row(a, b, randomStr))
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)

            // first cluster, the outputLevel should be 5
            checkAnswer(spark.sql("CALL paimon.sys.compact(table => 'T')"), Row(true) :: Nil)

            // first cluster result
            val result2 = new util.ArrayList[Row]()
            result2.add(0, Row(0, 0, randomStr))
            result2.add(1, Row(0, 1, randomStr))
            result2.add(2, Row(1, 0, randomStr))
            result2.add(3, Row(1, 1, randomStr))
            result2.add(4, Row(0, 2, randomStr))
            result2.add(5, Row(1, 2, randomStr))
            result2.add(6, Row(2, 0, randomStr))
            result2.add(7, Row(2, 1, randomStr))
            result2.add(8, Row(2, 2, randomStr))

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result2)

            var clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            var dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(0).level()).isEqualTo(5)

            // second write
            inputData.addData((0, 3, null), (1, 3, null), (2, 3, null))
            inputData.addData((3, 0, null), (3, 1, null), (3, 2, null), (3, 3, null))
            stream.processAllAvailable()

            val result3 = new util.ArrayList[Row]()
            result3.addAll(result2)
            for (a <- 0 until 3) {
              result3.add(Row(a, 3, null))
            }
            for (b <- 0 until 4) {
              result3.add(Row(3, b, null))
            }

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result3)

            // second cluster, the outputLevel should be 4
            checkAnswer(spark.sql("CALL paimon.sys.compact(table => 'T')"), Row(true) :: Nil)
            // second cluster result, level-5 and level-4 are individually ordered
            val result4 = new util.ArrayList[Row]()
            result4.addAll(result2)
            result4.add(Row(0, 3, null))
            result4.add(Row(1, 3, null))
            result4.add(Row(3, 0, null))
            result4.add(Row(3, 1, null))
            result4.add(Row(2, 3, null))
            result4.add(Row(3, 2, null))
            result4.add(Row(3, 3, null))
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result4)

            clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().size()).isEqualTo(2)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(0).level()).isEqualTo(5)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(1).level()).isEqualTo(4)

            // full cluster
            checkAnswer(
              spark.sql("CALL paimon.sys.compact(table => 'T', compact_strategy => 'full')"),
              Row(true) :: Nil)
            val result5 = new util.ArrayList[Row]()
            result5.add(Row(0, 0, randomStr))
            result5.add(Row(0, 1, randomStr))
            result5.add(Row(1, 0, randomStr))
            result5.add(Row(1, 1, randomStr))
            result5.add(Row(0, 2, randomStr))
            result5.add(Row(0, 3, null))
            result5.add(Row(1, 2, randomStr))
            result5.add(Row(1, 3, null))
            result5.add(Row(2, 0, randomStr))
            result5.add(Row(2, 1, randomStr))
            result5.add(Row(3, 0, null))
            result5.add(Row(3, 1, null))
            result5.add(Row(2, 2, randomStr))
            result5.add(Row(2, 3, null))
            result5.add(Row(3, 2, null))
            result5.add(Row(3, 3, null))
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result5)

            clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(0).level()).isEqualTo(5)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: cluster for partitioned table") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b INT, c STRING, pt INT)
               |PARTITIONED BY (pt)
               |TBLPROPERTIES ('bucket'='-1', 'num-levels'='6', 'num-sorted-run.compaction-trigger'='2', 'clustering.columns'='a,b', 'clustering.strategy'='zorder', 'clustering.incremental' = 'true')
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int, String, Int)]
          val stream = inputData
            .toDS()
            .toDF("a", "b", "c", "pt")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T ORDER BY pt")

          try {
            val random = new Random()
            val randomStr = random.nextString(50)
            // first write
            for (pt <- 0 until 2) {
              val c = if (pt == 0) randomStr else null
              inputData.addData((0, 0, c, pt))
              inputData.addData((0, 1, c, pt))
              inputData.addData((0, 2, c, pt))
              inputData.addData((1, 0, c, pt))
              inputData.addData((1, 1, c, pt))
              inputData.addData((1, 2, c, pt))
              inputData.addData((2, 0, c, pt))
              inputData.addData((2, 1, c, pt))
              inputData.addData((2, 2, c, pt))
            }
            stream.processAllAvailable()

            val result = new util.ArrayList[Row]()
            for (pt <- 0 until 2) {
              for (a <- 0 until 3) {
                for (b <- 0 until 3) {
                  val c = if (pt == 0) randomStr else null
                  result.add(Row(a, b, c, pt))
                }
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)

            // first cluster, the outputLevel should be 5
            checkAnswer(spark.sql("CALL paimon.sys.compact(table => 'T')"), Row(true) :: Nil)

            // first cluster result
            val result2 = new util.ArrayList[Row]()
            for (pt <- 0 until 2) {
              val c = if (pt == 0) randomStr else null
              result2.add(Row(0, 0, c, pt))
              result2.add(Row(0, 1, c, pt))
              result2.add(Row(1, 0, c, pt))
              result2.add(Row(1, 1, c, pt))
              result2.add(Row(0, 2, c, pt))
              result2.add(Row(1, 2, c, pt))
              result2.add(Row(2, 0, c, pt))
              result2.add(Row(2, 1, c, pt))
              result2.add(Row(2, 2, c, pt))
            }

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result2)

            var clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            var dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(2)
            dataSplits.forEach(
              dataSplit => {
                Assertions.assertThat(dataSplit.dataFiles().size()).isEqualTo(1)
                Assertions.assertThat(dataSplit.dataFiles().get(0).level()).isEqualTo(5)
              })

            // second write
            for (pt <- 0 until 2) {
              inputData.addData((0, 3, null, pt), (1, 3, null, pt), (2, 3, null, pt))
              inputData.addData(
                (3, 0, null, pt),
                (3, 1, null, pt),
                (3, 2, null, pt),
                (3, 3, null, pt))
            }
            stream.processAllAvailable()

            val result3 = new util.ArrayList[Row]()
            for (pt <- 0 until 2) {
              val c = if (pt == 0) randomStr else null
              result3.add(Row(0, 0, c, pt))
              result3.add(Row(0, 1, c, pt))
              result3.add(Row(1, 0, c, pt))
              result3.add(Row(1, 1, c, pt))
              result3.add(Row(0, 2, c, pt))
              result3.add(Row(1, 2, c, pt))
              result3.add(Row(2, 0, c, pt))
              result3.add(Row(2, 1, c, pt))
              result3.add(Row(2, 2, c, pt))
              for (a <- 0 until 3) {
                result3.add(Row(a, 3, null, pt))
              }
              for (b <- 0 until 4) {
                result3.add(Row(3, b, null, pt))
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result3)

            // second cluster
            checkAnswer(spark.sql("CALL paimon.sys.compact(table => 'T')"), Row(true) :: Nil)
            val result4 = new util.ArrayList[Row]()
            // for partition-0: only file in level-0 will be picked for clustering, outputLevel is 4
            result4.add(Row(0, 0, randomStr, 0))
            result4.add(Row(0, 1, randomStr, 0))
            result4.add(Row(1, 0, randomStr, 0))
            result4.add(Row(1, 1, randomStr, 0))
            result4.add(Row(0, 2, randomStr, 0))
            result4.add(Row(1, 2, randomStr, 0))
            result4.add(Row(2, 0, randomStr, 0))
            result4.add(Row(2, 1, randomStr, 0))
            result4.add(Row(2, 2, randomStr, 0))
            result4.add(Row(0, 3, null, 0))
            result4.add(Row(1, 3, null, 0))
            result4.add(Row(3, 0, null, 0))
            result4.add(Row(3, 1, null, 0))
            result4.add(Row(2, 3, null, 0))
            result4.add(Row(3, 2, null, 0))
            result4.add(Row(3, 3, null, 0))
            // for partition-1:all files will be picked for clustering, outputLevel is 5
            result4.add(Row(0, 0, null, 1))
            result4.add(Row(0, 1, null, 1))
            result4.add(Row(1, 0, null, 1))
            result4.add(Row(1, 1, null, 1))
            result4.add(Row(0, 2, null, 1))
            result4.add(Row(0, 3, null, 1))
            result4.add(Row(1, 2, null, 1))
            result4.add(Row(1, 3, null, 1))
            result4.add(Row(2, 0, null, 1))
            result4.add(Row(2, 1, null, 1))
            result4.add(Row(3, 0, null, 1))
            result4.add(Row(3, 1, null, 1))
            result4.add(Row(2, 2, null, 1))
            result4.add(Row(2, 3, null, 1))
            result4.add(Row(3, 2, null, 1))
            result4.add(Row(3, 3, null, 1))

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result4)

            clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(2)
            dataSplits.forEach(
              dataSplit => {
                if (dataSplit.partition().getInt(0) == 1) {
                  // partition-1
                  Assertions.assertThat(dataSplit.dataFiles().size()).isEqualTo(1)
                  Assertions.assertThat(dataSplit.dataFiles().get(0).level()).isEqualTo(5)
                } else {
                  // partition-0
                  Assertions.assertThat(dataSplit.dataFiles().size()).isEqualTo(2)
                  Assertions.assertThat(dataSplit.dataFiles().get(0).level()).isEqualTo(5)
                  Assertions.assertThat(dataSplit.dataFiles().get(1).level()).isEqualTo(4)
                }
              })
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: cluster for partitioned table with partition filter") {
    sql(
      """
        |CREATE TABLE T (a INT, b INT, pt INT)
        |PARTITIONED BY (pt)
        |TBLPROPERTIES (
        |  'bucket'='-1', 'num-levels'='6', 'num-sorted-run.compaction-trigger'='2',
        |  'clustering.columns'='a,b', 'clustering.strategy'='zorder', 'clustering.incremental' = 'true'
        |)
        |""".stripMargin)

    sql("INSERT INTO T VALUES (0, 0, 0), (0, 0, 1)")
    sql("INSERT INTO T VALUES (0, 1, 0), (0, 1, 1)")
    sql("INSERT INTO T VALUES (0, 2, 0), (0, 2, 1)")
    sql("INSERT INTO T VALUES (1, 0, 0), (1, 0, 1)")
    sql("INSERT INTO T VALUES (1, 1, 0), (1, 1, 1)")
    sql("INSERT INTO T VALUES (1, 2, 0), (1, 2, 1)")
    sql("INSERT INTO T VALUES (2, 0, 0), (2, 0, 1)")
    sql("INSERT INTO T VALUES (2, 1, 0), (2, 1, 1)")
    sql("INSERT INTO T VALUES (2, 2, 0), (2, 2, 1)")

    sql("CALL sys.compact(table => 'T', where => 'pt = 0')")
    checkAnswer(
      sql("select distinct partition, level from `T$files` order by partition"),
      Seq(Row("{0}", 5), Row("{1}", 0))
    )

    sql("CALL sys.compact(table => 'T', where => 'pt = 1')")
    checkAnswer(
      sql("select distinct partition, level from `T$files` order by partition"),
      Seq(Row("{0}", 5), Row("{1}", 5))
    )
  }

  test("Paimon Procedure: cluster with deletion vectors") {
    failAfter(Span(5, org.scalatest.time.Minutes)) {
      withTempDir {
        checkpointDir =>
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b INT, c STRING)
               |TBLPROPERTIES ('bucket'='-1', 'deletion-vectors.enabled'='true','num-levels'='6', 'num-sorted-run.compaction-trigger'='2', 'clustering.columns'='a,b', 'clustering.strategy'='zorder', 'clustering.incremental' = 'true')
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, Int, String)]
          val stream = inputData
            .toDS()
            .toDF("a", "b", "c")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            val random = new Random()
            val randomStr = random.nextString(40)
            // first write
            inputData.addData((0, 0, randomStr))
            inputData.addData((0, 1, randomStr))
            inputData.addData((0, 2, randomStr))
            inputData.addData((1, 0, randomStr))
            inputData.addData((1, 1, randomStr))
            inputData.addData((1, 2, randomStr))
            inputData.addData((2, 0, randomStr))
            inputData.addData((2, 1, randomStr))
            inputData.addData((2, 2, randomStr))
            stream.processAllAvailable()

            val result = new util.ArrayList[Row]()
            for (a <- 0 until 3) {
              for (b <- 0 until 3) {
                result.add(Row(a, b, randomStr))
              }
            }
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result)

            // first cluster, the outputLevel should be 5
            checkAnswer(spark.sql("CALL paimon.sys.compact(table => 'T')"), Row(true) :: Nil)

            // first cluster result
            val result2 = new util.ArrayList[Row]()
            result2.add(0, Row(0, 0, randomStr))
            result2.add(1, Row(0, 1, randomStr))
            result2.add(2, Row(1, 0, randomStr))
            result2.add(3, Row(1, 1, randomStr))
            result2.add(4, Row(0, 2, randomStr))
            result2.add(5, Row(1, 2, randomStr))
            result2.add(6, Row(2, 0, randomStr))
            result2.add(7, Row(2, 1, randomStr))
            result2.add(8, Row(2, 2, randomStr))

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result2)

            var clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            var dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(0).level()).isEqualTo(5)

            // second write
            inputData.addData((0, 3, null), (1, 3, null), (2, 3, null))
            inputData.addData((3, 0, null), (3, 1, null), (3, 2, null), (3, 3, null))
            stream.processAllAvailable()

            // delete (0,0), which is in level-5 file
            spark.sql("DELETE FROM T WHERE a=0 and b=0;").collect()
            // delete (0,3), which is in level-0 file
            spark.sql("DELETE FROM T WHERE a=0 and b=3;").collect()

            val result3 = new util.ArrayList[Row]()
            result3.addAll(result2.subList(1, result2.size()))
            for (a <- 1 until 3) {
              result3.add(Row(a, 3, null))
            }
            for (b <- 0 until 4) {
              result3.add(Row(3, b, null))
            }

            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result3)

            // second cluster, the outputLevel should be 4. dv index for level-0 will be updated
            // and dv index for level-5 will be retained
            checkAnswer(spark.sql("CALL paimon.sys.compact(table => 'T')"), Row(true) :: Nil)
            // second cluster result, level-5 and level-4 are individually ordered
            val result4 = new util.ArrayList[Row]()
            result4.addAll(result2.subList(1, result2.size()))
            result4.add(Row(1, 3, null))
            result4.add(Row(3, 0, null))
            result4.add(Row(3, 1, null))
            result4.add(Row(2, 3, null))
            result4.add(Row(3, 2, null))
            result4.add(Row(3, 3, null))
            Assertions.assertThat(query().collect()).containsExactlyElementsOf(result4)

            clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().size()).isEqualTo(2)
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(0).level()).isEqualTo(5)
            Assertions.assertThat(dataSplits.get(0).deletionFiles().get().get(0)).isNotNull
            Assertions.assertThat(dataSplits.get(0).dataFiles().get(1).level()).isEqualTo(4)
            Assertions.assertThat(dataSplits.get(0).deletionFiles().get().get(1)).isNull()

            // full cluster
            checkAnswer(
              spark.sql("CALL paimon.sys.compact(table => 'T', compact_strategy => 'full')"),
              Row(true) :: Nil)
            clusteredTable = loadTable("T")
            checkSnapshot(clusteredTable)
            dataSplits = clusteredTable.newSnapshotReader().read().dataSplits()
            Assertions.assertThat(dataSplits.size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).dataFiles().size()).isEqualTo(1)
            Assertions.assertThat(dataSplits.get(0).deletionFiles().get().get(0)).isNull()

          } finally {
            stream.stop()
          }
      }
    }
  }

  def checkSnapshot(table: FileStoreTable): Unit = {
    Assertions
      .assertThat(table.latestSnapshot().get().commitKind().toString)
      .isEqualTo(CommitKind.COMPACT.toString)
  }

  def lastSnapshotCommand(table: FileStoreTable): CommitKind = {
    table.snapshotManager().latestSnapshot().commitKind()
  }

  def lastSnapshotId(table: FileStoreTable): Long = {
    table.snapshotManager().latestSnapshotId()
  }
}
