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

import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.DateTimeUtils

import org.apache.spark.sql.Row

import java.util.concurrent.TimeUnit

class RemoveOrphanFilesProcedureTest extends PaimonSparkTestBase {

  private val ORPHAN_FILE_1 = "bucket-0/orphan_file1"
  private val ORPHAN_FILE_2 = "bucket-0/orphan_file2"

  test("Paimon procedure: remove orphan files") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a'), ('2', 'b')")

    val table = loadTable("T")
    val fileIO = table.fileIO()
    val tablePath = table.location()

    val orphanFile1 = new Path(tablePath, ORPHAN_FILE_1)
    val orphanFile2 = new Path(tablePath, ORPHAN_FILE_2)

    fileIO.tryToWriteAtomic(orphanFile1, "a")
    Thread.sleep(2000)
    fileIO.tryToWriteAtomic(orphanFile2, "b")

    // by default, no file deleted
    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'T')"), Row(0, 0) :: Nil)

    val orphanFile2ModTime = fileIO.getFileStatus(orphanFile2).getModificationTime
    val older_than1 = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(
        orphanFile2ModTime -
          TimeUnit.SECONDS.toMillis(1)),
      3)

    checkAnswer(
      spark.sql(s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than1')"),
      Row(1, 1) :: Nil)

    val older_than2 = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
      3)

    checkAnswer(
      spark.sql(s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than2')"),
      Row(1, 1) :: Nil)

    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'T')"), Row(0, 0) :: Nil)
  }

  test("Paimon procedure: dry run remove orphan files") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a'), ('2', 'b')")

    val table = loadTable("T")
    val fileIO = table.fileIO()
    val tablePath = table.location()

    val orphanFile1 = new Path(tablePath, ORPHAN_FILE_1)
    val orphanFile2 = new Path(tablePath, ORPHAN_FILE_2)

    fileIO.writeFile(orphanFile1, "a", true)
    Thread.sleep(2000)
    fileIO.writeFile(orphanFile2, "b", true)

    // by default, no file deleted
    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'T')"), Row(0, 0) :: Nil)

    val older_than = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
      3)

    checkAnswer(
      spark.sql(
        s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than', dry_run => true)"),
      Row(2, 2) :: Nil
    )

    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'T')"), Row(0, 0) :: Nil)
  }

  test("Paimon procedure: remove database orphan files") {
    spark.sql(s"""
                 |CREATE TABLE T1 (id STRING, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)
    spark.sql(s"INSERT INTO T1 VALUES ('1', 'a'), ('2', 'b')")

    spark.sql(s"""
                 |CREATE TABLE T2 (id STRING, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)
    spark.sql(s"INSERT INTO T2 VALUES ('1', 'a'), ('2', 'b')")

    val table1 = loadTable("T1")
    val table2 = loadTable("T2")
    val fileIO1 = table1.fileIO()
    val fileIO2 = table2.fileIO()
    val tablePath1 = table1.location()
    val tablePath2 = table2.location()

    val orphanFile11 = new Path(tablePath1, ORPHAN_FILE_1)
    val orphanFile12 = new Path(tablePath1, ORPHAN_FILE_2)
    val orphanFile21 = new Path(tablePath2, ORPHAN_FILE_1)
    val orphanFile22 = new Path(tablePath2, ORPHAN_FILE_2)

    fileIO1.tryToWriteAtomic(orphanFile11, "a")
    fileIO2.tryToWriteAtomic(orphanFile21, "a")
    Thread.sleep(2000)
    fileIO1.tryToWriteAtomic(orphanFile12, "b")
    fileIO2.tryToWriteAtomic(orphanFile22, "b")

    // by default, no file deleted
    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'test.*')"), Row(0, 0) :: Nil)

    val orphanFile12ModTime = fileIO1.getFileStatus(orphanFile12).getModificationTime
    val older_than1 = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(
        orphanFile12ModTime -
          TimeUnit.SECONDS.toMillis(1)),
      3)

    checkAnswer(
      spark.sql(s"CALL sys.remove_orphan_files(table => 'test.*', older_than => '$older_than1')"),
      Row(2, 2) :: Nil
    )

    val older_than2 = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
      3)

    checkAnswer(
      spark.sql(s"CALL sys.remove_orphan_files(table => 'test.*', older_than => '$older_than2')"),
      Row(2, 2) :: Nil
    )

    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'test.*')"), Row(0, 0) :: Nil)
  }

  test("Paimon procedure: remove orphan files with mode") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a'), ('2', 'b')")

    val table = loadTable("T")
    val fileIO = table.fileIO()
    val tablePath = table.location()

    val orphanFile1 = new Path(tablePath, ORPHAN_FILE_1)
    val orphanFile2 = new Path(tablePath, ORPHAN_FILE_2)

    fileIO.tryToWriteAtomic(orphanFile1, "a")
    Thread.sleep(2000)
    fileIO.tryToWriteAtomic(orphanFile2, "b")

    // by default, no file deleted
    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'T')"), Row(0, 0) :: Nil)

    val orphanFile2ModTime = fileIO.getFileStatus(orphanFile2).getModificationTime
    val older_than1 = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(
        orphanFile2ModTime -
          TimeUnit.SECONDS.toMillis(1)),
      3)

    checkAnswer(
      spark.sql(
        s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than1', mode => 'diSTributed')"),
      Row(1, 1) :: Nil)

    val older_than2 = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
      3)

    checkAnswer(
      spark.sql(
        s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than2', mode => 'local')"),
      Row(1, 1) :: Nil)

    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'T')"), Row(0, 0) :: Nil)
  }

  test("Paimon procedure: remove orphan files with data file path directory") {
    sql(s"""
           |CREATE TABLE T (id STRING, name STRING)
           |USING PAIMON
           |TBLPROPERTIES ('primary-key'='id', 'data-file.path-directory'='data')
           |""".stripMargin)

    sql(s"INSERT INTO T VALUES ('1', 'a'), ('2', 'b')")

    val table = loadTable("T")
    val orphanFile = new Path(table.store().pathFactory().dataFilePath(), ORPHAN_FILE_1)
    table.fileIO().tryToWriteAtomic(orphanFile, "b")

    Thread.sleep(1000)
    val older_than = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
      3)
    checkAnswer(
      sql(s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than')"),
      Row(1, 1) :: Nil)
  }

  test("Paimon procedure: clean empty directory after removing orphan files") {
    spark.sql("""
                |CREATE TABLE T (k STRING, pt STRING)
                |using paimon TBLPROPERTIES ('primary-key'='k,pt', 'bucket'='1',
                |'snapshot.clean-empty-directories'='false') PARTITIONED BY (pt);
                |""".stripMargin)

    spark.sql("""
                |insert into T values
                |("a", "2024-06-02"),("b", "2024-06-02"),("d", "2024-06-03"),
                |("c", "2024-06-01"),("Never-expire", "9999-09-09");
                |
                |""".stripMargin)

    // by default, no file deleted
    checkAnswer(spark.sql(s"CALL sys.remove_orphan_files(table => 'T')"), Row(0, 0) :: Nil)

    spark.sql(
      "CALL sys.expire_partitions(table => 'T' , " +
        "expiration_time => '1 d', timestamp_formatter => 'yyyy-MM-dd', max_expires => 3);")

    // insert a new snapshot to clean expired partitioned files
    spark.sql("insert into T values ('Never-expire-2', '9999-09-09')")

    val table = loadTable("T")
    val fileIO = table.fileIO()
    val tablePath = table.location()

    val partitionValue = "pt=2024-06-01"
    val partitionPath = tablePath + "/" + partitionValue
    val orphanFile1 = new Path(partitionPath, ORPHAN_FILE_1)
    val orphanFile2 = new Path(partitionPath, ORPHAN_FILE_2)
    fileIO.writeFile(orphanFile1, "a", true)
    Thread.sleep(2000)
    fileIO.writeFile(orphanFile2, "b", true)

    checkAnswer(
      spark.sql("CALL paimon.sys.expire_snapshots(table => 'T', retain_max => 1)"),
      Row(2) :: Nil)

    val older_than1 = new java.sql.Timestamp(
      fileIO.getFileStatus(orphanFile2).getModificationTime - TimeUnit.SECONDS.toMillis(1))

    // partition 'pt=2024-06-01' has one orphan file left
    assertResult(true)(
      fileIO
        .listDirectories(tablePath)
        .map(status => status.getPath.getName)
        .contains(partitionValue))

    checkAnswer(
      spark.sql(
        s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than1', mode => 'distributed')"),
      Row(1, 1) :: Nil)

    val older_than2 = new java.sql.Timestamp(System.currentTimeMillis())

    checkAnswer(
      spark.sql(
        s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than2', mode => 'local')"),
      Row(1, 1) :: Nil)

    // partition 'pt=2024-06-01' has no orphan files, clean empty directory
    assertResult(false)(
      fileIO
        .listDirectories(tablePath)
        .map(status => status.getPath.getName)
        .contains(partitionValue))
  }

}
