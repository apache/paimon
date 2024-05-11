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

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.deletionvectors.{DeletionVector, DeletionVectorsMaintainer}
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions

import scala.util.Random

class DeletionVectorTest extends PaimonSparkTestBase {

  test("Paimon deletionVector2: deletion vector write verification") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, name STRING)
                   |TBLPROPERTIES (
                   | 'bucket' = '1',
                   | 'file.format' = 'parquet',
                   | 'deletion-vectors.enabled' = 'true'
                   |)
                   |""".stripMargin)
      val table = loadTable("T")

      // Insert1
      // f1 (1, 2, 3), row with positions 0 and 2 in f1 are marked deleted
      // f2 (1, 3)
      spark.sql("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
//      spark.sql("INSERT INTO T VALUES (1, 'a_new1'), (3, 'c_new1')")
      spark.sql("DELETE FROM T WHERE id = 4")
      checkAnswer(
        spark.sql(s"SELECT * from T ORDER BY id"),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  test("Paimon deletionVector: deletion vector write verification") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, name STRING)
                   |TBLPROPERTIES (
                   | 'bucket' = '1',
                   | 'primary-key' = 'id',
                   | 'file.format' = 'parquet',
                   | 'deletion-vectors.enabled' = 'true'
                   |)
                   |""".stripMargin)
      val table = loadTable("T")

      // Insert1
      // f1 (1, 2, 3), row with positions 0 and 2 in f1 are marked deleted
      // f2 (1, 3)
      spark.sql("INSERT INTO T VALUES (1, 'aaaaaaaaaaaaaaaaaaa'), (2, 'b'), (3, 'c')")
      spark.sql("INSERT INTO T VALUES (1, 'a_new1'), (3, 'c_new1')")
      checkAnswer(
        spark.sql(s"SELECT * from T ORDER BY id"),
        Row(1, "a_new1") :: Row(2, "b") :: Row(3, "c_new1") :: Nil)

      val dvMaintainerFactory =
        new DeletionVectorsMaintainer.Factory(table.store().newIndexFileHandler())

      def restoreDeletionVectors(): java.util.Map[String, DeletionVector] = {
        dvMaintainerFactory
          .createOrRestore(table.snapshotManager().latestSnapshotId(), BinaryRow.EMPTY_ROW, 0)
          .deletionVectors()
      }

      val deletionVectors1 = restoreDeletionVectors()
      // 1, 3 deleted, their row positions are 0, 2
      Assertions.assertEquals(1, deletionVectors1.size())
      deletionVectors1
        .entrySet()
        .forEach(
          e => {
            Assertions.assertTrue(e.getValue.isDeleted(0))
            Assertions.assertTrue(e.getValue.isDeleted(2))
          })

      // Compact
      // f3 (1, 2, 3), no deletion
      spark.sql("CALL sys.compact('T')")
      val deletionVectors2 = restoreDeletionVectors()
      // After compaction, deletionVectors should be empty
      Assertions.assertTrue(deletionVectors2.isEmpty)

      // Insert2
      // f3 (1, 2, 3), row with position 1 in f3 is marked deleted
      // f4 (2)
      spark.sql("INSERT INTO T VALUES (2, 'b_new2')")
      checkAnswer(
        spark.sql(s"SELECT * from T ORDER BY id"),
        Row(1, "a_new1") :: Row(2, "b_new2") :: Row(3, "c_new1") :: Nil)

      val deletionVectors3 = restoreDeletionVectors()
      // 2 deleted, row positions is 1
      Assertions.assertEquals(1, deletionVectors3.size())
      deletionVectors3
        .entrySet()
        .forEach(
          e => {
            Assertions.assertTrue(e.getValue.isDeleted(1))
          })
    }
  }

  test("Paimon deletionVector: e2e random write") {
    val bucket = Random.shuffle(Seq("-1", "1", "3")).head
    val changelogProducer = Random.shuffle(Seq("none", "lookup")).head
    val format = Random.shuffle(Seq("orc", "parquet", "avro")).head
    val batchSize = Random.nextInt(1024) + 1

    val dvTbl = "deletion_vector_tbl"
    val resultTbl = "result_tbl"
    spark.sql(s"drop table if exists $dvTbl")
    spark.sql(s"""
                 |CREATE TABLE $dvTbl (id INT, name STRING, pt STRING)
                 |TBLPROPERTIES (
                 | 'primary-key' = 'id, pt',
                 | 'deletion-vectors.enabled' = 'true',
                 | 'bucket' = '$bucket',
                 | 'changelog-producer' = '$changelogProducer',
                 | 'file.format' = '$format',
                 | 'read.batch-size' = '$batchSize'
                 |)
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    spark.sql(s"drop table if exists $resultTbl")
    spark.sql(s"""
                 |CREATE TABLE $resultTbl (id INT, name STRING, pt STRING)
                 |TBLPROPERTIES (
                 | 'primary-key' = 'id, pt',
                 | 'deletion-vectors.enabled' = 'false'
                 |)
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    def insert(t1: String, t2: String, count: Int): Unit = {
      val ids = (1 to count).map(_ => Random.nextInt(10000))
      val names = (1 to count).map(_ => (Random.nextInt(26) + 'a'.toInt).toChar.toString)
      val pts = (1 to count).map(_ => s"p${Random.nextInt(3)}")
      val values = ids
        .zip(names)
        .zip(pts)
        .map { case ((id, name), pt) => s"($id, '$name', '$pt')" }
        .mkString(", ")
      spark.sql(s"INSERT INTO $t1 VALUES $values")
      spark.sql(s"INSERT INTO $t2 VALUES $values")
    }

    def delete(t1: String, t2: String, count: Int): Unit = {
      val ids = (1 to count).map(_ => Random.nextInt(10000)).toList
      val idsString = ids.mkString(", ")
      spark.sql(s"DELETE FROM $t1 WHERE id IN ($idsString)")
      spark.sql(s"DELETE FROM $t2 WHERE id IN ($idsString)")
    }

    def update(t1: String, t2: String, count: Int): Unit = {
      val ids = (1 to count).map(_ => Random.nextInt(10000)).toList
      val idsString = ids.mkString(", ")
      val randomName = (Random.nextInt(26) + 'a'.toInt).toChar.toString
      spark.sql(s"UPDATE $t1 SET name = '$randomName' WHERE id IN ($idsString)")
      spark.sql(s"UPDATE $t2 SET name = '$randomName' WHERE id IN ($idsString)")
    }

    def checkResult(t1: String, t2: String): Unit = {
      try {
        checkAnswer(
          spark.sql(s"SELECT * FROM $t1 ORDER BY id, pt"),
          spark.sql(s"SELECT * FROM $t2 ORDER BY id, pt"))
      } catch {
        case e: Throwable =>
          println(s"test error, table params: ${loadTable(dvTbl).options()}")
          throw new RuntimeException(e)
      }
    }

    val operations = Seq(
      () => insert(dvTbl, resultTbl, 1000),
      () => update(dvTbl, resultTbl, 100),
      () => delete(dvTbl, resultTbl, 100)
    )

    // Insert first
    operations.head()
    checkResult(dvTbl, resultTbl)

    for (_ <- 1 to 20) {
      // Randomly select an operation
      operations(Random.nextInt(operations.size))()
      checkResult(dvTbl, resultTbl)
    }
  }
}
