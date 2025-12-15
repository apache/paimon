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

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions

import java.util.{Arrays, Collections, Map => JMap}

import scala.collection.JavaConverters._

/** Tests for the rescale procedure. See [[RescaleProcedure]]. */
class RescaleProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: rescale basic functionality") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='2')
                   |""".stripMargin)

      val table = loadTable("T")
      spark.sql(s"INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

      val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      val initialSnapshotId = lastSnapshotId(table)

      // Rescale with explicit bucket_num
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('bucket' = '4')")
      checkAnswer(spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4)"), Row(true) :: Nil)

      val reloadedTable = loadTable("T")
      Assertions.assertThat(lastSnapshotCommand(reloadedTable)).isEqualTo(CommitKind.OVERWRITE)
      Assertions.assertThat(lastSnapshotId(reloadedTable)).isGreaterThan(initialSnapshotId)
      Assertions.assertThat(getBucketCount(reloadedTable)).isEqualTo(4)

      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData).containsExactlyElementsOf(Arrays.asList(initialData: _*))

      // Rescale without bucket_num (use current bucket)
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('bucket' = '3')")
      checkAnswer(spark.sql("CALL sys.rescale(table => 'T')"), Row(true) :: Nil)
      Assertions.assertThat(getBucketCount(loadTable("T"))).isEqualTo(3)

      // Rescale with explicit bucket_num
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('bucket' = '4')")
      checkAnswer(spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4)"), Row(true) :: Nil)
      Assertions.assertThat(getBucketCount(loadTable("T"))).isEqualTo(4)

      // Decrease bucket count (4 -> 2)
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('bucket' = '2')")
      checkAnswer(spark.sql("CALL sys.rescale(table => 'T', bucket_num => 2)"), Row(true) :: Nil)
      val reloadedTableAfterDecrease = loadTable("T")
      Assertions.assertThat(getBucketCount(reloadedTableAfterDecrease)).isEqualTo(2)
      reloadedTableAfterDecrease.newSnapshotReader.read.dataSplits.asScala.toList.foreach(
        split => Assertions.assertThat(split.bucket()).isLessThan(2))

      // Verify data integrity after bucket decrease
      val afterDecreaseData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions
        .assertThat(afterDecreaseData)
        .containsExactlyElementsOf(Arrays.asList(initialData: _*))
    }
  }

  test("Paimon Procedure: rescale partitioned tables") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING, pt STRING, dt STRING, hh INT)
                   |TBLPROPERTIES ('primary-key'='id, pt, dt, hh', 'bucket'='2')
                   |PARTITIONED BY (pt, dt, hh)
                   |""".stripMargin)

      val table = loadTable("T")
      spark.sql(
        s"INSERT INTO T VALUES (1, 'a', 'p1', '2024-01-01', 0), (2, 'b', 'p1', '2024-01-01', 0)")
      spark.sql(
        s"INSERT INTO T VALUES (3, 'c', 'p2', '2024-01-01', 1), (4, 'd', 'p2', '2024-01-02', 0)")

      val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      val initialSnapshotId = lastSnapshotId(table)

      // Rescale single partition field
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('bucket' = '4')")
      checkAnswer(
        spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4, partitions => 'pt=\"p1\"')"),
        Row(true) :: Nil)

      val reloadedTable = loadTable("T")
      Assertions.assertThat(lastSnapshotCommand(reloadedTable)).isEqualTo(CommitKind.OVERWRITE)
      Assertions.assertThat(lastSnapshotId(reloadedTable)).isGreaterThan(initialSnapshotId)

      val p1Predicate = PartitionPredicate.fromMap(
        reloadedTable.schema().logicalPartitionType(),
        Collections.singletonMap("pt", "p1"),
        reloadedTable.coreOptions().partitionDefaultName())
      val p1Splits = reloadedTable.newSnapshotReader
        .withPartitionFilter(p1Predicate)
        .read
        .dataSplits
        .asScala
        .toList
      p1Splits.foreach(split => Assertions.assertThat(split.bucket()).isLessThan(4))

      // Rescale multiple partition fields
      val snapshotBeforeTest2 = lastSnapshotId(reloadedTable)
      checkAnswer(
        spark.sql(
          "CALL sys.rescale(table => 'T', bucket_num => 4, partitions => 'dt=\"2024-01-01\",hh=0')"),
        Row(true) :: Nil)

      val reloadedTable2 = loadTable("T")
      Assertions.assertThat(lastSnapshotCommand(reloadedTable2)).isEqualTo(CommitKind.OVERWRITE)
      Assertions.assertThat(lastSnapshotId(reloadedTable2)).isGreaterThan(snapshotBeforeTest2)

      // Rescale empty partition (should not create new snapshot)
      val snapshotBeforeEmpty = lastSnapshotId(reloadedTable2)
      checkAnswer(
        spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4, partitions => 'pt=\"p3\"')"),
        Row(true) :: Nil)
      Assertions.assertThat(lastSnapshotId(loadTable("T"))).isEqualTo(snapshotBeforeEmpty)

      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData).containsExactlyElementsOf(Arrays.asList(initialData: _*))
    }
  }

  test("Paimon Procedure: rescale with where clause") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, value STRING, dt STRING, hh INT)
                   |TBLPROPERTIES ('primary-key'='id, dt, hh', 'bucket'='2')
                   |PARTITIONED BY (dt, hh)
                   |""".stripMargin)

      val table = loadTable("T")
      spark.sql(s"INSERT INTO T VALUES (1, 'a', '2024-01-01', 0), (2, 'b', '2024-01-01', 0)")
      spark.sql(s"INSERT INTO T VALUES (3, 'c', '2024-01-01', 1), (4, 'd', '2024-01-01', 1)")
      spark.sql(s"INSERT INTO T VALUES (5, 'e', '2024-01-02', 0), (6, 'f', '2024-01-02', 1)")

      val initialData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      val initialSnapshotId = lastSnapshotId(table)

      // Test 1: Rescale with where clause using single partition column
      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('bucket' = '4')")
      checkAnswer(
        spark.sql(
          "CALL sys.rescale(table => 'T', bucket_num => 4, where => 'dt = \"2024-01-01\"')"),
        Row(true) :: Nil)

      val reloadedTable = loadTable("T")
      Assertions.assertThat(lastSnapshotCommand(reloadedTable)).isEqualTo(CommitKind.OVERWRITE)
      Assertions.assertThat(lastSnapshotId(reloadedTable)).isGreaterThan(initialSnapshotId)

      // Test 2: Rescale with where clause using multiple partition columns
      val snapshotBeforeTest2 = lastSnapshotId(reloadedTable)
      checkAnswer(
        spark.sql(
          "CALL sys.rescale(table => 'T', bucket_num => 4, where => 'dt = \"2024-01-01\" AND hh >= 1')"),
        Row(true) :: Nil)

      val reloadedTable2 = loadTable("T")
      Assertions.assertThat(lastSnapshotCommand(reloadedTable2)).isEqualTo(CommitKind.OVERWRITE)
      Assertions.assertThat(lastSnapshotId(reloadedTable2)).isGreaterThan(snapshotBeforeTest2)

      // Verify data integrity
      val afterData = spark.sql("SELECT * FROM T ORDER BY id").collect()
      Assertions.assertThat(afterData).containsExactlyElementsOf(Arrays.asList(initialData: _*))
    }
  }

  test("Paimon Procedure: rescale with ALTER TABLE and write validation") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (f0 INT)
                   |TBLPROPERTIES ('bucket'='2', 'bucket-key'='f0')
                   |""".stripMargin)

      val table = loadTable("T")

      spark.sql(s"INSERT INTO T VALUES (1), (2), (3), (4), (5)")

      val snapshot = lastSnapshotId(table)
      Assertions.assertThat(snapshot).isGreaterThanOrEqualTo(0)

      val initialBuckets = getBucketCount(table)
      Assertions.assertThat(initialBuckets).isEqualTo(2)

      val initialData = spark.sql("SELECT * FROM T ORDER BY f0").collect()
      Assertions.assertThat(initialData.length).isEqualTo(5)

      spark.sql("ALTER TABLE T SET TBLPROPERTIES ('bucket' = '4')")

      val reloadedTable = loadTable("T")
      val newBuckets = getBucketCount(reloadedTable)
      Assertions.assertThat(newBuckets).isEqualTo(4)

      val afterAlterData = spark.sql("SELECT * FROM T ORDER BY f0").collect()
      val initialDataList = Arrays.asList(initialData: _*)
      Assertions.assertThat(afterAlterData).containsExactlyElementsOf(initialDataList)

      val writeError = intercept[org.apache.spark.SparkException] {
        spark.sql("INSERT INTO T VALUES (6)")
      }
      val errorMessage = Option(writeError.getMessage).getOrElse("")
      val causeMessage =
        Option(writeError.getCause).flatMap(c => Option(c.getMessage)).getOrElse("")
      val expectedMessage =
        "Try to write table with a new bucket num 4, but the previous bucket num is 2"
      val fullMessage = Seq(errorMessage, causeMessage).filter(_.nonEmpty).mkString(" ")
      Assertions
        .assertThat(fullMessage)
        .contains(expectedMessage)

      checkAnswer(spark.sql("CALL sys.rescale(table => 'T', bucket_num => 4)"), Row(true) :: Nil)

      val finalTable = loadTable("T")
      val finalSnapshot = lastSnapshotId(finalTable)
      Assertions.assertThat(finalSnapshot).isGreaterThan(snapshot)
      Assertions.assertThat(lastSnapshotCommand(finalTable)).isEqualTo(CommitKind.OVERWRITE)

      val finalBuckets = getBucketCount(finalTable)
      Assertions.assertThat(finalBuckets).isEqualTo(4)

      val afterRescaleData = spark.sql("SELECT * FROM T ORDER BY f0").collect()
      Assertions.assertThat(afterRescaleData).containsExactlyElementsOf(initialDataList)

      spark.sql("INSERT INTO T VALUES (6)")
      val finalData = spark.sql("SELECT * FROM T ORDER BY f0").collect()
      Assertions.assertThat(finalData.length).isEqualTo(6)
    }
  }

  test("Paimon Procedure: rescale error cases") {
    // Table with no snapshot
    withTable("T1") {
      spark.sql(s"""
                   |CREATE TABLE T1 (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='2')
                   |""".stripMargin)
      assert(intercept[IllegalArgumentException] {
        spark.sql("CALL sys.rescale(table => 'T1', bucket_num => 4)")
      }.getMessage.contains("has no snapshot"))
    }

    // Postpone bucket table requires bucket_num
    withTable("T2") {
      spark.sql(s"""
                   |CREATE TABLE T2 (id INT, value STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='-2')
                   |""".stripMargin)
      spark.sql(s"INSERT INTO T2 VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      assert(
        intercept[IllegalArgumentException] {
          spark.sql("CALL sys.rescale(table => 'T2')")
        }.getMessage.contains(
          "When rescaling postpone bucket tables, you must provide the resulting bucket number"))
      checkAnswer(spark.sql("CALL sys.rescale(table => 'T2', bucket_num => 4)"), Row(true) :: Nil)
    }

    // partitions and where cannot be used together
    withTable("T3") {
      spark.sql(s"""
                   |CREATE TABLE T3 (id INT, value STRING, pt STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='2')
                   |PARTITIONED BY (pt)
                   |""".stripMargin)
      spark.sql(s"INSERT INTO T3 VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
      assert(intercept[IllegalArgumentException] {
        spark.sql(
          "CALL sys.rescale(table => 'T3', bucket_num => 4, partitions => 'pt=\"p1\"', where => 'pt = \"p1\"')")
      }.getMessage.contains("partitions and where cannot be used together"))
    }

    // where clause with non-partition column should fail
    withTable("T4") {
      spark.sql(s"""
                   |CREATE TABLE T4 (id INT, value STRING, pt STRING)
                   |TBLPROPERTIES ('primary-key'='id', 'bucket'='2')
                   |PARTITIONED BY (pt)
                   |""".stripMargin)
      spark.sql(s"INSERT INTO T4 VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
      assert(intercept[IllegalArgumentException] {
        spark.sql("CALL sys.rescale(table => 'T4', bucket_num => 4, where => 'id = 1')")
      }.getMessage.contains("Only partition predicate is supported"))
    }
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
