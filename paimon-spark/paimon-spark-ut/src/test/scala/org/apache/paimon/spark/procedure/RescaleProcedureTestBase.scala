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
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions

import java.util.{Collections, Map => JMap}

import scala.collection.JavaConverters._

/** Test rescale procedure. See [[RescaleProcedure]]. */
abstract class RescaleProcedureTestBase extends PaimonSparkTestBase {

  import testImplicits._

  // ----------------------- Basic Rescale -----------------------

  test("Paimon Procedure: rescale non-partitioned table") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='2')
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(s"INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

      val initialBuckets = getBucketCount(table)
      Assertions.assertThat(initialBuckets).isEqualTo(2)

      val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      val initialSnapshotId = lastSnapshotId(table)

      checkAnswer(spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4)"), Row(true) :: Nil)

      val reloadedTable = loadTable("T")

      Assertions.assertThat(lastSnapshotCommand(reloadedTable)).isEqualTo(CommitKind.OVERWRITE)
      Assertions.assertThat(lastSnapshotId(reloadedTable)).isGreaterThan(initialSnapshotId)

      val newBuckets = getBucketCount(reloadedTable)
      Assertions.assertThat(newBuckets).isEqualTo(4)

      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData).containsExactlyElementsOf(initialData)

      val dataSplits = reloadedTable.newSnapshotReader.read.dataSplits.asScala.toList
      dataSplits.foreach(
        split => {
          Assertions.assertThat(split.bucket()).isLessThan(4)
        })
    }
  }

  test("Paimon Procedure: rescale partitioned table") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING, pt STRING)
                   |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                   |PARTITIONED BY (pt)
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(
        s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2'), (4, 'd', 'p2')")

      val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      val initialSnapshotId = lastSnapshotId(table)

      checkAnswer(
        spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4, partition => 'pt=\"p1\"')"),
        Row(true) :: Nil)

      val reloadedTable = loadTable("T")
      Assertions.assertThat(lastSnapshotCommand(reloadedTable)).isEqualTo(CommitKind.OVERWRITE)
      Assertions.assertThat(lastSnapshotId(reloadedTable)).isGreaterThan(initialSnapshotId)

      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData).containsExactlyElementsOf(initialData)

      val p1PartitionPredicate = PartitionPredicate.fromMap(
        reloadedTable.schema().logicalPartitionType(),
        Collections.singletonMap("pt", "p1"),
        reloadedTable.coreOptions().partitionDefaultName())
      val p1Splits = reloadedTable.newSnapshotReader
        .withPartitionFilter(p1PartitionPredicate)
        .read
        .dataSplits
        .asScala
        .toList
      p1Splits.foreach(
        split => {
          Assertions.assertThat(split.bucket()).isLessThan(4)
        })

      val p2PartitionPredicate = PartitionPredicate.fromMap(
        reloadedTable.schema().logicalPartitionType(),
        Collections.singletonMap("pt", "p2"),
        reloadedTable.coreOptions().partitionDefaultName())
      val p2Splits = reloadedTable.newSnapshotReader
        .withPartitionFilter(p2PartitionPredicate)
        .read
        .dataSplits
        .asScala
        .toList
      p2Splits.foreach(
        split => {
          Assertions.assertThat(split.bucket()).isLessThan(2)
        })
    }
  }

  test("Paimon Procedure: rescale with scan_parallelism and sink_parallelism") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='2')
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(s"INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")

      val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()

      checkAnswer(
        spark.sql(
          "CALL sys.rescale(table => 'T', bucket_num => 4, scan_parallelism => 3, sink_parallelism => 5)"),
        Row(true) :: Nil)

      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData).containsExactlyElementsOf(initialData)

      val reloadedTable = loadTable("T")

      val newBuckets = getBucketCount(reloadedTable)
      Assertions.assertThat(newBuckets).isEqualTo(4)
    }
  }

  test("Paimon Procedure: rescale without bucket_num (use current bucket)") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='3')
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(s"INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

      val initialBuckets = getBucketCount(table)
      Assertions.assertThat(initialBuckets).isEqualTo(3)

      val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()

      checkAnswer(spark.sql("CALL sys.rescale(table => 'T')"), Row(true) :: Nil)

      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData).containsExactlyElementsOf(initialData)

      val reloadedTable = loadTable("T")

      val newBuckets = getBucketCount(reloadedTable)
      Assertions.assertThat(newBuckets).isEqualTo(3)
    }
  }

  test("Paimon Procedure: rescale partition with multiple partitions") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING, dt STRING, hh INT)
                   |TBLPROPERTIES ('primary-key'='id, dt, hh', 'bucket'='2')
                   |PARTITIONED BY (dt, hh)
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(s"INSERT INTO T VALUES (1, 'a', '2024-01-01', 0), (2, 'b', '2024-01-01', 0)")
      spark.sql(s"INSERT INTO T VALUES (3, 'c', '2024-01-01', 1), (4, 'd', '2024-01-01', 1)")
      spark.sql(s"INSERT INTO T VALUES (5, 'e', '2024-01-02', 0), (6, 'f', '2024-01-02', 0)")

      val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      val initialSnapshotId = lastSnapshotId(table)

      checkAnswer(
        spark.sql(
          "CALL sys.rescale(table => 'T', bucket_num => 4, partition => 'dt=\"2024-01-01\",hh=0')"),
        Row(true) :: Nil)

      Assertions.assertThat(lastSnapshotCommand(table)).isEqualTo(CommitKind.OVERWRITE)
      Assertions.assertThat(lastSnapshotId(table)).isGreaterThan(initialSnapshotId)

      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData).containsExactlyElementsOf(initialData)

      val reloadedTable = loadTable("T")

      val targetPartitionMap: JMap[String, String] = new java.util.HashMap[String, String]()
      targetPartitionMap.put("dt", "2024-01-01")
      targetPartitionMap.put("hh", "0")
      val targetPartitionPredicate = PartitionPredicate.fromMap(
        reloadedTable.schema().logicalPartitionType(),
        targetPartitionMap,
        reloadedTable.coreOptions().partitionDefaultName())
      val targetPartitionSplits = reloadedTable.newSnapshotReader
        .withPartitionFilter(targetPartitionPredicate)
        .read
        .dataSplits
        .asScala
        .toList
      targetPartitionSplits.foreach(
        split => {
          Assertions.assertThat(split.bucket()).isLessThan(4)
        })
    }
  }

  test("Paimon Procedure: rescale table with no snapshot") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='2')
                   |""".stripMargin)

      val table = loadTable("T")

      assert(intercept[IllegalArgumentException] {
        spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4)")
      }.getMessage.contains("has no snapshot"))
    }
  }

  test("Paimon Procedure: rescale postpone bucket table requires bucket_num") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='-1')
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(s"INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      assert(
        intercept[IllegalArgumentException] {
          spark.sql("CALL sys.rescale(table => 'T')")
        }.getMessage.contains(
          "When rescaling postpone bucket tables, you must provide the resulting bucket number"))

      checkAnswer(spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4)"), Row(true) :: Nil)

      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData.length).isEqualTo(3)
    }
  }

  test("Paimon Procedure: rescale empty partition") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING, pt STRING)
                   |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                   |PARTITIONED BY (pt)
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p1')")

      val initialSnapshotId = lastSnapshotId(table)

      checkAnswer(
        spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4, partition => 'pt=\"p2\"')"),
        Row(true) :: Nil)

      Assertions.assertThat(lastSnapshotId(table)).isEqualTo(initialSnapshotId)
    }
  }

  test("Paimon Procedure: rescale reduces bucket count") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='4')
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(
        s"INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h')")

      val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      val initialBuckets = getBucketCount(table)
      Assertions.assertThat(initialBuckets).isEqualTo(4)

      checkAnswer(spark.sql("CALL sys.rescale(table => 'T', bucket_num => 2)"), Row(true) :: Nil)

      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData).containsExactlyElementsOf(initialData)

      val reloadedTable = loadTable("T")

      val newBuckets = getBucketCount(reloadedTable)
      Assertions.assertThat(newBuckets).isEqualTo(2)

      val dataSplits = reloadedTable.newSnapshotReader.read.dataSplits.asScala.toList
      dataSplits.foreach(
        split => {
          Assertions.assertThat(split.bucket()).isLessThan(2)
        })
    }
  }

  test("Paimon Procedure: rescale aware bucket table") {
    Seq(1, 2, 3).foreach(
      initialBucket => {
        withTable("T") {
          spark.sql(s"""
                       |CREATE TABLE T (id INT, value STRING)
                       |TBLPROPERTIES ('primary-key'='id', 'bucket'='$initialBucket')
                       |""".stripMargin)

          val table = loadTable("T")

          for (i <- 1 to 10) {
            spark.sql(s"INSERT INTO T VALUES ($i, 'value$i')")
          }

          val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()
          val initialBuckets = getBucketCount(table)
          Assertions.assertThat(initialBuckets).isEqualTo(initialBucket)

          val targetBucket = if (initialBucket == 1) 3 else 1
          checkAnswer(
            spark.sql(s"CALL sys.rescale(table => 'T', bucket_num => $targetBucket)"),
            Row(true) :: Nil)

          val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
          Assertions.assertThat(afterData).containsExactlyElementsOf(initialData)

          val reloadedTable = loadTable("T")

          val newBuckets = getBucketCount(reloadedTable)
          Assertions.assertThat(newBuckets).isEqualTo(targetBucket)
        }
      })
  }

  // ----------------------- Helper Methods -----------------------

  def getBucketCount(table: FileStoreTable): Int = {
    val bucketOption = table.coreOptions().bucket()
    if (bucketOption == -1) {
      val dataSplits = table.newSnapshotReader.read.dataSplits.asScala.toList
      if (dataSplits.isEmpty) {
        -1
      } else {
        dataSplits.map(_.bucket()).max + 1
      }
    } else {
      bucketOption
    }
  }

  def lastSnapshotCommand(table: FileStoreTable): CommitKind = {
    table.snapshotManager().latestSnapshot().commitKind()
  }

  def lastSnapshotId(table: FileStoreTable): Long = {
    table.snapshotManager().latestSnapshotId()
  }
}
