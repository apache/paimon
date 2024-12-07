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
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.{PaimonSparkTestBase, PaimonSplitScan}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.util.QueryExecutionListener
import org.junit.jupiter.api.Assertions

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class DeletionVectorTest extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

  private def runAndCheckSplitScan(query: String): Unit = {
    val batchScans = new ArrayBuffer[(DataSourceV2Relation, BatchScanExec)]()
    val listener = new QueryExecutionListener {
      override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}

      private def isValidSplitScan(scan: BatchScanExec): Boolean = {
        if (!scan.scan.isInstanceOf[PaimonSplitScan]) {
          return false
        }
        val splitScan = scan.scan.asInstanceOf[PaimonSplitScan]
        assert(splitScan.table.primaryKeys().isEmpty)
        splitScan.coreOptions.deletionVectorsEnabled() &&
        scan.output.exists(
          attr => PaimonMetadataColumn.SUPPORTED_METADATA_COLUMNS.contains(attr.name))
      }

      private def appendScan(qe: QueryExecution, plan: SparkPlan): Unit = {
        plan match {
          case memory: InMemoryTableScanExec =>
            foreach(memory.relation.cachedPlan)(p => appendScan(qe, p))
          case scan: BatchScanExec if isValidSplitScan(scan) =>
            val logicalScan = qe.analyzed.find(_.isInstanceOf[DataSourceV2Relation])
            assert(logicalScan.isDefined)
            batchScans.append((logicalScan.get.asInstanceOf[DataSourceV2Relation], scan))
          case _ =>
        }
      }

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        foreach(qe.executedPlan)(p => appendScan(qe, p))
      }
    }
    spark.listenerManager.register(listener)

    try {
      val df = spark.sql(query)
      df.collect()
      Utils.waitUntilEventEmpty(df.sparkSession)
      assert(batchScans.nonEmpty, query)
      assert(
        batchScans.forall {
          case (logicalScan, scan) =>
            logicalScan.output.size > scan.output.size
        },
        batchScans)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  bucketModes.foreach {
    bucket =>
      test(s"Paimon DeletionVector: merge into with bucket = $bucket") {
        withTable("source", "target") {
          val bucketKey = if (bucket > 1) {
            ", 'bucket-key' = 'a'"
          } else {
            ""
          }
          Seq((1, 100, "c11"), (3, 300, "c33"), (5, 500, "c55"), (7, 700, "c77"), (9, 900, "c99"))
            .toDF("a", "b", "c")
            .createOrReplaceTempView("source")

          spark.sql(
            s"""
               |CREATE TABLE target (a INT, b INT, c STRING)
               |TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'bucket' = '$bucket' $bucketKey)
               |""".stripMargin)
          spark.sql(
            "INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3'), (4, 40, 'c4'), (5, 50, 'c5')")

          val table = loadTable("target")
          val dvMaintainerFactory =
            new DeletionVectorsMaintainer.Factory(table.store().newIndexFileHandler())
          runAndCheckSplitScan(s"""
                                  |MERGE INTO target
                                  |USING source
                                  |ON target.a = source.a
                                  |WHEN MATCHED AND target.a = 5 THEN
                                  |UPDATE SET b = source.b + target.b
                                  |WHEN MATCHED AND source.c > 'c2' THEN
                                  |UPDATE SET *
                                  |WHEN MATCHED THEN
                                  |DELETE
                                  |WHEN NOT MATCHED AND c > 'c9' THEN
                                  |INSERT (a, b, c) VALUES (a, b * 1.1, c)
                                  |WHEN NOT MATCHED THEN
                                  |INSERT *
                                  |""".stripMargin)

          checkAnswer(
            spark.sql("SELECT * FROM target ORDER BY a, b"),
            Row(2, 20, "c2") :: Row(3, 300, "c33") :: Row(4, 40, "c4") :: Row(5, 550, "c5") :: Row(
              7,
              700,
              "c77") :: Row(9, 990, "c99") :: Nil
          )
          val deletionVectors = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          Assertions.assertTrue(deletionVectors.nonEmpty)
        }
      }
  }

  bucketModes.foreach {
    bucket =>
      test(
        s"Paimon DeletionVector: update for append non-partitioned table with bucket = $bucket") {
        withTable("T") {
          val bucketKey = if (bucket > 1) {
            ", 'bucket-key' = 'id'"
          } else {
            ""
          }
          spark.sql(s"""
                       |CREATE TABLE T (id INT, name STRING)
                       |TBLPROPERTIES (
                       |  'deletion-vectors.enabled' = 'true',
                       |  'compaction.max.file-num' = '50',
                       |  'bucket' = '$bucket' $bucketKey)
                       |""".stripMargin)

          val table = loadTable("T")
          val dvMaintainerFactory =
            new DeletionVectorsMaintainer.Factory(table.store().newIndexFileHandler())

          spark.sql("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')")
          val deletionVectors1 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          Assertions.assertEquals(0, deletionVectors1.size)

          val cond1 = "id = 2"
          val rowMetaInfo1 = getFilePathAndRowIndex(cond1)
          runAndCheckSplitScan(s"UPDATE T SET name = 'b_2' WHERE $cond1")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a") :: Row(2, "b_2") :: Row(3, "c") :: Nil)
          val deletionVectors2 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          Assertions.assertEquals(1, deletionVectors2.size)
          deletionVectors2
            .foreach {
              case (filePath, dv) =>
                rowMetaInfo1(filePath).foreach(index => Assertions.assertTrue(dv.isDeleted(index)))
            }

          spark.sql("INSERT INTO T VALUES (4, 'd'), (5, 'e')")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a") :: Row(2, "b_2") :: Row(3, "c") :: Row(4, "d") :: Row(5, "e") :: Nil)
          val deletionVectors3 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          Assertions.assertTrue(deletionVectors2 == deletionVectors3)

          val cond2 = "id % 2 = 1"
          runAndCheckSplitScan(s"UPDATE T SET name = concat(name, '_2') WHERE $cond2")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a_2") :: Row(2, "b_2") :: Row(3, "c_2") :: Row(4, "d") :: Row(5, "e_2") :: Nil)

          runAndCheckSplitScan("UPDATE T SET name = '_all'")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "_all") :: Row(2, "_all") :: Row(3, "_all") :: Row(4, "_all") :: Row(
              5,
              "_all") :: Nil)

          spark.sql("CALL sys.compact('T')")
          val deletionVectors4 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          // After compaction, deletionVectors should be empty
          Assertions.assertTrue(deletionVectors4.isEmpty)
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "_all") :: Row(2, "_all") :: Row(3, "_all") :: Row(4, "_all") :: Row(
              5,
              "_all") :: Nil)
        }
      }
  }

  bucketModes.foreach {
    bucket =>
      test(s"Paimon DeletionVector: update for append partitioned table with bucket = $bucket") {
        withTable("T") {
          val bucketKey = if (bucket > 1) {
            ", 'bucket-key' = 'id'"
          } else {
            ""
          }
          spark.sql(
            s"""
               |CREATE TABLE T (id INT, name STRING, pt STRING)
               |PARTITIONED BY(pt)
               |TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'bucket' = '$bucket' $bucketKey)
               |""".stripMargin)

          val table = loadTable("T")
          val dvMaintainerFactory =
            new DeletionVectorsMaintainer.Factory(table.store().newIndexFileHandler())

          spark.sql(
            "INSERT INTO T VALUES (1, 'a', '2024'), (2, 'b', '2024'), (3, 'c', '2025'), (4, 'd', '2025')")
          val deletionVectors1 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          Assertions.assertEquals(0, deletionVectors1.size)

          val cond1 = "id = 2"
          val rowMetaInfo1 = getFilePathAndRowIndex(cond1)
          runAndCheckSplitScan(s"UPDATE T SET name = 'b_2' WHERE $cond1")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a", "2024") :: Row(2, "b_2", "2024") :: Row(3, "c", "2025") :: Row(
              4,
              "d",
              "2025") :: Nil)
          val deletionVectors2 =
            getLatestDeletionVectors(
              table,
              dvMaintainerFactory,
              Seq(BinaryRow.singleColumn("2024"), BinaryRow.singleColumn("2025")))
          Assertions.assertEquals(1, deletionVectors2.size)
          deletionVectors2
            .foreach {
              case (filePath, dv) =>
                rowMetaInfo1(filePath).foreach(index => Assertions.assertTrue(dv.isDeleted(index)))
            }

          val cond2 = "pt = '2025'"
          val rowMetaInfo2 = rowMetaInfo1 ++ getFilePathAndRowIndex(cond2)
          runAndCheckSplitScan(s"UPDATE T SET name = concat(name, '_2') WHERE $cond2")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a", "2024") :: Row(2, "b_2", "2024") :: Row(3, "c_2", "2025") :: Row(
              4,
              "d_2",
              "2025") :: Nil)
          val deletionVectors3 =
            getLatestDeletionVectors(
              table,
              dvMaintainerFactory,
              Seq(BinaryRow.singleColumn("2024")))
          Assertions.assertTrue(deletionVectors2 == deletionVectors3)
          val deletionVectors4 =
            getLatestDeletionVectors(
              table,
              dvMaintainerFactory,
              Seq(BinaryRow.singleColumn("2024"), BinaryRow.singleColumn("2025")))
          deletionVectors4
            .foreach {
              case (filePath, dv) =>
                rowMetaInfo2(filePath).foreach(index => Assertions.assertTrue(dv.isDeleted(index)))
            }

          spark.sql("CALL sys.compact('T')")
          val deletionVectors5 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          // After compaction, deletionVectors should be empty
          Assertions.assertTrue(deletionVectors5.isEmpty)
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a", "2024") :: Row(2, "b_2", "2024") :: Row(3, "c_2", "2025") :: Row(
              4,
              "d_2",
              "2025") :: Nil)
        }
      }
  }

  bucketModes.foreach {
    bucket =>
      test(
        s"Paimon DeletionVector: delete for append non-partitioned table with bucket = $bucket") {
        withTable("T") {
          val bucketKey = if (bucket > 1) {
            ", 'bucket-key' = 'id'"
          } else {
            ""
          }
          spark.sql(
            s"""
               |CREATE TABLE T (id INT, name STRING)
               |TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'bucket' = '$bucket' $bucketKey)
               |""".stripMargin)

          val table = loadTable("T")
          val dvMaintainerFactory =
            new DeletionVectorsMaintainer.Factory(table.store().newIndexFileHandler())

          spark.sql("INSERT INTO T VALUES (1, 'a'), (2, 'b')")
          val deletionVectors1 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          Assertions.assertEquals(0, deletionVectors1.size)

          val cond1 = "id = 2"
          val rowMetaInfo1 = getFilePathAndRowIndex(cond1)
          runAndCheckSplitScan(s"DELETE FROM T WHERE $cond1")
          checkAnswer(spark.sql(s"SELECT * from T ORDER BY id"), Row(1, "a") :: Nil)
          val deletionVectors2 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          Assertions.assertEquals(1, deletionVectors2.size)
          deletionVectors2
            .foreach {
              case (filePath, dv) =>
                rowMetaInfo1(filePath).foreach(index => Assertions.assertTrue(dv.isDeleted(index)))
            }

          spark.sql("INSERT INTO T VALUES (2, 'bb'), (3, 'c'), (4, 'd')")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a") :: Row(2, "bb") :: Row(3, "c") :: Row(4, "d") :: Nil)
          val deletionVectors3 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          Assertions.assertTrue(deletionVectors2 == deletionVectors3)

          val cond2 = "id % 2 = 1"
          runAndCheckSplitScan(s"DELETE FROM T WHERE $cond2")
          checkAnswer(spark.sql(s"SELECT * from T ORDER BY id"), Row(2, "bb") :: Row(4, "d") :: Nil)

          spark.sql("CALL sys.compact('T')")
          val deletionVectors4 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          // After compaction, deletionVectors should be empty
          Assertions.assertTrue(deletionVectors4.isEmpty)
          checkAnswer(spark.sql(s"SELECT * from T ORDER BY id"), Row(2, "bb") :: Row(4, "d") :: Nil)
        }
      }
  }

  bucketModes.foreach {
    bucket =>
      test(s"Paimon DeletionVector: delete for append partitioned table with bucket = $bucket") {
        withTable("T") {
          val bucketKey = if (bucket > 1) {
            ", 'bucket-key' = 'id'"
          } else {
            ""
          }
          spark.sql(
            s"""
               |CREATE TABLE T (id INT, name STRING, pt STRING)
               |PARTITIONED BY(pt)
               |TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'bucket' = '$bucket' $bucketKey)
               |""".stripMargin)

          val table = loadTable("T")
          val dvMaintainerFactory =
            new DeletionVectorsMaintainer.Factory(table.store().newIndexFileHandler())

          def getDeletionVectors(ptValues: Seq[String]): Map[String, DeletionVector] = {
            getLatestDeletionVectors(
              table,
              dvMaintainerFactory,
              ptValues.map(BinaryRow.singleColumn))
          }

          spark.sql(
            "INSERT INTO T VALUES (1, 'a', '2024'), (2, 'b', '2024'), (3, 'c', '2025'), (4, 'd', '2025')")
          val deletionVectors1 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
          Assertions.assertEquals(0, deletionVectors1.size)

          val cond1 = "id = 2"
          val rowMetaInfo1 = getFilePathAndRowIndex(cond1)
          runAndCheckSplitScan(s"DELETE FROM T WHERE $cond1")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a", "2024") :: Row(3, "c", "2025") :: Row(4, "d", "2025") :: Nil)
          val deletionVectors2 = getDeletionVectors(Seq("2024", "2025"))
          Assertions.assertEquals(1, deletionVectors2.size)
          deletionVectors2
            .foreach {
              case (filePath, dv) =>
                rowMetaInfo1(filePath).foreach(index => Assertions.assertTrue(dv.isDeleted(index)))
            }

          val cond2 = "id = 3"
          val rowMetaInfo2 = rowMetaInfo1 ++ getFilePathAndRowIndex(cond2)
          runAndCheckSplitScan(s"DELETE FROM T WHERE $cond2")
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a", "2024") :: Row(4, "d", "2025") :: Nil)
          val deletionVectors3 = getDeletionVectors(Seq("2024"))
          Assertions.assertTrue(deletionVectors2 == deletionVectors3)
          val deletionVectors4 = getDeletionVectors(Seq("2024", "2025"))
          deletionVectors4
            .foreach {
              case (filePath, dv) =>
                rowMetaInfo2(filePath).foreach(index => Assertions.assertTrue(dv.isDeleted(index)))
            }

          spark.sql("""CALL sys.compact(table => 'T', partitions => "pt = '2024'")""")
          Assertions.assertTrue(getDeletionVectors(Seq("2024")).isEmpty)
          Assertions.assertTrue(getDeletionVectors(Seq("2025")).nonEmpty)
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a", "2024") :: Row(4, "d", "2025") :: Nil)

          spark.sql("""CALL sys.compact(table => 'T', where => "pt = '2025'")""")
          Assertions.assertTrue(getDeletionVectors(Seq("2025")).isEmpty)
          Assertions.assertTrue(getDeletionVectors(Seq("2025")).isEmpty)
          checkAnswer(
            spark.sql(s"SELECT * from T ORDER BY id"),
            Row(1, "a", "2024") :: Row(4, "d", "2025") :: Nil)
        }
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

      val deletionVectors1 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
      // 1, 3 deleted, their row positions are 0, 2
      Assertions.assertEquals(1, deletionVectors1.size)
      deletionVectors1
        .foreach {
          case (_, dv) =>
            Assertions.assertTrue(dv.isDeleted(0))
            Assertions.assertTrue(dv.isDeleted(2))
        }

      // Compact
      // f3 (1, 2, 3), no deletion
      spark.sql("CALL sys.compact('T')")
      val deletionVectors2 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
      // After compaction, deletionVectors should be empty
      Assertions.assertTrue(deletionVectors2.isEmpty)

      // Insert2
      // f3 (1, 2, 3), row with position 1 in f3 is marked deleted
      // f4 (2)
      spark.sql("INSERT INTO T VALUES (2, 'b_new2')")
      checkAnswer(
        spark.sql(s"SELECT * from T ORDER BY id"),
        Row(1, "a_new1") :: Row(2, "b_new2") :: Row(3, "c_new1") :: Nil)

      val deletionVectors3 = getAllLatestDeletionVectors(table, dvMaintainerFactory)
      // 2 deleted, row positions is 1
      Assertions.assertEquals(1, deletionVectors3.size)
      deletionVectors3
        .foreach {
          case (_, dv) =>
            Assertions.assertTrue(dv.isDeleted(1))
        }
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

  test("Paimon deletionVector: select with format filter push down") {
    val format = Random.shuffle(Seq("parquet", "orc", "avro")).head
    val blockSize = Random.nextInt(10240) + 1
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING)
                 |TBLPROPERTIES (
                 | 'primary-key' = 'id',
                 | 'deletion-vectors.enabled' = 'true',
                 | 'file.format' = '$format',
                 | 'file.block-size' = '${blockSize}b',
                 | 'bucket' = '1'
                 |)
                 |""".stripMargin)

    spark
      .range(1, 10001)
      .toDF("id")
      .withColumn("name", lit("a"))
      .createOrReplaceTempView("source_tbl")

    sql("INSERT INTO T SELECT * FROM source_tbl")
    // update to create dv
    sql("INSERT INTO T VALUES (1111, 'a_new'), (2222, 'a_new')")

    checkAnswer(
      sql("SELECT count(*) FROM T"),
      Seq(10000).toDF()
    )
    checkAnswer(
      sql("SELECT count(*) FROM T WHERE (id > 1000 and id <= 2000) or (id > 3000 and id <= 4000)"),
      Seq(2000).toDF()
    )
    checkAnswer(
      spark.sql("SELECT name FROM T WHERE id = 1111"),
      Seq("a_new").toDF()
    )
    checkAnswer(
      spark.sql("SELECT name FROM T WHERE id = 2222"),
      Seq("a_new").toDF()
    )
  }

  private def getPathName(path: String): String = {
    new Path(path).getName
  }

  private def getAllLatestDeletionVectors(
      table: FileStoreTable,
      dvMaintainerFactory: DeletionVectorsMaintainer.Factory): Map[String, DeletionVector] = {
    getLatestDeletionVectors(table, dvMaintainerFactory, Seq(BinaryRow.EMPTY_ROW))
  }

  private def getLatestDeletionVectors(
      table: FileStoreTable,
      dvMaintainerFactory: DeletionVectorsMaintainer.Factory,
      partitions: Seq[BinaryRow]): Map[String, DeletionVector] = {
    partitions.flatMap {
      partition =>
        dvMaintainerFactory
          .createOrRestore(table.snapshotManager().latestSnapshotId(), partition)
          .deletionVectors()
          .asScala
    }.toMap
  }

  private def getFilePathAndRowIndex(condition: String): Map[String, Array[Long]] = {
    spark
      .sql(s"SELECT __paimon_file_path, __paimon_row_index FROM T WHERE $condition ORDER BY id")
      .as[(String, Long)]
      .collect()
      .groupBy(_._1)
      .map(kv => (getPathName(kv._1), kv._2.map(_._2)))
  }
}
