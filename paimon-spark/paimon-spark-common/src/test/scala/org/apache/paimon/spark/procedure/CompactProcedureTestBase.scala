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
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest
import org.assertj.core.api.Assertions

import java.util

import scala.collection.JavaConverters._

/** Test compact procedure. See [[CompactProcedure]]. */
abstract class CompactProcedureTestBase extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

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

  test("Paimon Procedure: sort compact with max_order_threads") {
    Seq("order", "zorder").foreach {
      orderStrategy =>
        {
          withTable("T") {
            spark.sql(s"""
                         |CREATE TABLE T (id INT, pt STRING)
                         |TBLPROPERTIES ('bucket'='-1')
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
                s"CALL sys.compact(table => 'T', order_strategy => '$orderStrategy', order_by => 'id', max_order_threads => 2)"),
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
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, value STRING, pt STRING)
         |TBLPROPERTIES ('bucket'='-1', 'write-only'='true', 'compaction.min.file-num'='2', 'compaction.max.file-num' = '3')
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
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, value STRING, pt STRING)
         |TBLPROPERTIES ('bucket'='-1', 'write-only'='true', 'compaction.max.file-num' = '10')
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
  }

  test("Paimon Procedure: compact with where") {
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, value STRING, dt STRING, hh INT)
         |TBLPROPERTIES ('bucket'='1', 'bucket-key'='id', 'write-only'='true', 'compaction.min.file-num'='1', 'compaction.max.file-num'='2')
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

    val where = CompactProcedure.toWhere(conditions)
    val whereExpected =
      "(f0=0 AND f1=0 AND f2=0) OR (f0=1 AND f1=1 AND f2=1) OR (f0=1 AND f1=2 AND f2=2) OR (f3=3)"

    Assertions.assertThat(where).isEqualTo(whereExpected)
  }

  def lastSnapshotCommand(table: FileStoreTable): CommitKind = {
    table.snapshotManager().latestSnapshot().commitKind()
  }

  def lastSnapshotId(table: FileStoreTable): Long = {
    table.snapshotManager().latestSnapshotId()
  }
}
