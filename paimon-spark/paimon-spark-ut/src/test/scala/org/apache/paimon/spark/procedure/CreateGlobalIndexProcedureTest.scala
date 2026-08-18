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

import org.apache.paimon.globalindex.{KeySerializer, SortedIndexFileMeta}
import org.apache.paimon.index.DataEvolutionIndexSourceMeta
import org.apache.paimon.manifest.IndexManifestEntry
import org.apache.paimon.memory.MemorySlice
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.VarCharType
import org.apache.paimon.utils.Range

import org.apache.spark.sql.Row
import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.streaming.StreamTest

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.immutable

class CreateGlobalIndexProcedureTest extends PaimonSparkTestBase with StreamTest {

  test("refresh btree index after data evolution update") {
    withTable("T", "S", "P") {
      spark.sql("""
                  |CREATE TABLE T (id INT, idx INT, payload STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.enabled' = 'true',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true',
                  |  'global-index.column-update-action' = 'IGNORE',
                  |  'btree-index.records-per-range' = '2')
                  |""".stripMargin)

      spark.sql(
        s"INSERT INTO T VALUES ${(0 until 10).map(i => s"($i, $i, 'p$i')").mkString(",")}"
      )
      createBTreeIndex("T", "idx")
      spark.sql(
        s"INSERT INTO T VALUES ${(10 until 20).map(i => s"($i, $i, 'p$i')").mkString(",")}"
      )
      createBTreeIndex("T", "idx")

      def entriesByRange: Map[String, Seq[IndexManifestEntry]] = {
        loadTable("T")
          .store()
          .newIndexFileHandler()
          .scan("btree")
          .asScala
          .groupBy(
            entry =>
              s"${entry.indexFile().globalIndexMeta().rowRangeStart()}:" +
                s"${entry.indexFile().globalIndexMeta().rowRangeEnd()}")
          .map { case (range, entries) => range -> entries.toList }
      }

      def fileNames(entries: Seq[IndexManifestEntry]): Set[String] =
        entries.map(_.indexFile().fileName()).toSet

      val initial = entriesByRange
      assert(initial.keySet == Set("0:9", "10:19"))
      assert(initial("0:9").size > 1)
      assert(initial("10:19").size > 1)
      val initialFirstFiles = fileNames(initial("0:9"))
      val initialSecondFiles = fileNames(initial("10:19"))

      spark.sql("CREATE TABLE S (id INT, idx INT)")
      spark.sql("INSERT INTO S VALUES (1, 1001)")
      spark.sql("""
                  |MERGE INTO T
                  |USING S
                  |ON T.id = S.id
                  |WHEN MATCHED THEN UPDATE SET T.idx = S.idx
                  |""".stripMargin)
      val updateSnapshotId = loadTable("T").snapshotManager().latestSnapshot().id()

      createBTreeIndex("T", "idx")
      assert(loadTable("T").snapshotManager().latestSnapshot().id() == updateSnapshotId + 1)

      val refreshed = entriesByRange
      assert(refreshed.keySet == Set("0:9", "10:19"))
      assert((fileNames(refreshed("0:9")).intersect(initialFirstFiles)).isEmpty)
      assert(fileNames(refreshed("10:19")) == initialSecondFiles)
      refreshed("0:9").foreach(
        entry =>
          assert(
            DataEvolutionIndexSourceMeta
              .fromIndexFile(entry.indexFile())
              .scanSnapshotId() == updateSnapshotId
          ))
      checkAnswer(sql("SELECT id FROM T WHERE idx = 1"), Seq.empty)
      checkAnswer(sql("SELECT id FROM T WHERE idx = 1001"), Seq(Row(1)))

      val refreshedSnapshotId = loadTable("T").snapshotManager().latestSnapshot().id()
      val refreshedFiles = refreshed.values.flatten.map(_.indexFile().fileName()).toSet
      createBTreeIndex("T", "idx")
      assert(loadTable("T").snapshotManager().latestSnapshot().id() == refreshedSnapshotId)
      assert(entriesByRange.values.flatten.map(_.indexFile().fileName()).toSet == refreshedFiles)

      spark.sql("CREATE TABLE P (id INT, payload STRING)")
      spark.sql("INSERT INTO P VALUES (1, 'new-payload')")
      spark.sql("""
                  |MERGE INTO T
                  |USING P
                  |ON T.id = P.id
                  |WHEN MATCHED THEN UPDATE SET T.payload = P.payload
                  |""".stripMargin)
      val payloadUpdateSnapshotId = loadTable("T").snapshotManager().latestSnapshot().id()
      createBTreeIndex("T", "idx")
      assert(loadTable("T").snapshotManager().latestSnapshot().id() == payloadUpdateSnapshotId)
      assert(entriesByRange.values.flatten.map(_.indexFile().fileName()).toSet == refreshedFiles)
    }
  }

  test("create btree global index") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, name STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true',
                  |  'btree-index.records-per-range' = '1000')
                  |""".stripMargin)

      val values =
        (0 until 100000).map(i => s"($i, 'name_$i')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output =
        spark
          .sql(
            "CALL sys.create_global_index(table => 'test.T', index_column => 'name', index_type => 'btree'," +
              " options => 'btree-index.records-per-range=1000')")
          .collect()
          .head

      assert(output.getBoolean(0))
      val table = loadTable("T")
      val btreeEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == "btree")
        .map(_.indexFile())
      assert(btreeEntries.nonEmpty)

      // 1. assert total row count, file count and row range
      val totalRowCount = btreeEntries.map(_.rowCount()).sum
      assert(btreeEntries.size == 100)
      assert(totalRowCount == 100000L)
      assert(btreeEntries.head.globalIndexMeta().rowRangeStart() == 0L)
      assert(btreeEntries.head.globalIndexMeta().rowRangeEnd() == 99999L)

      // 2. assert global index meta not null
      btreeEntries.foreach(e => assert(e.globalIndexMeta() != null))

      // 3. assert btree index file range non-overlapping
      case class MetaWithKey(meta: SortedIndexFileMeta, first: Object, last: Object)
      val keySerializer = KeySerializer.create(new VarCharType())
      val comparator = keySerializer.createComparator()

      def deserialize(bytes: Array[Byte]): Object = {
        keySerializer.deserialize(MemorySlice.wrap(bytes))
      }

      val btreeMetas = btreeEntries
        .map(_.globalIndexMeta().indexMeta())
        .map(meta => SortedIndexFileMeta.deserialize(meta))
        .map(
          m => {
            assert(m.getFirstKey != null)
            assert(m.getLastKey != null)
            MetaWithKey(m, deserialize(m.getFirstKey), deserialize(m.getLastKey))
          })

      // sort by first key
      val sorted = btreeMetas.sortWith((m1, m2) => comparator.compare(m1.first, m2.first) < 0)

      // should not overlap
      sorted.sliding(2).foreach {
        case Seq(prev: MetaWithKey, next: MetaWithKey) =>
          assert(comparator.compare(prev.last, next.first) <= 0)
        case _ => // ignore
      }
    }
  }

  test("create bitmap global index") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, name STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values =
        (0 until 10000).map(i => s"($i, 'name_$i')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output =
        spark
          .sql(
            "CALL sys.create_global_index(table => 'test.T', index_column => 'name', index_type => 'bitmap'," +
              " options => 'sorted-index.records-per-range=1000')")
          .collect()
          .head

      assert(output.getBoolean(0))
      val table = loadTable("T")
      val bitmapEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == "bitmap")
        .map(_.indexFile())
      assert(bitmapEntries.nonEmpty)
      assert(bitmapEntries.map(_.rowCount()).sum == 10000L)
      bitmapEntries.foreach(e => assert(e.globalIndexMeta() != null))
    }
  }

  test("create multivalue global index") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, tags ARRAY<STRING>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.enabled' = 'true',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      spark.sql(
        "INSERT INTO T VALUES " +
          "(1, array('red', 'blue')), " +
          "(2, array('blue')), " +
          "(3, array('green')), " +
          "(4, CAST(NULL AS ARRAY<STRING>)), " +
          "(5, array('red', 'red'))"
      )

      val output =
        spark
          .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'tags', " +
            "index_type => 'multivalue', options => 'sorted-index.records-per-range=2')")
          .collect()
          .head

      assert(output.getBoolean(0))
      val entries = loadTable("T")
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .map(_.indexFile())
        .filter(_.indexType() == "multivalue")
      assert(entries.size > 1)
      assert(entries.map(_.rowCount()).sum == 5L)
      entries.foreach(entry => assert(entry.globalIndexMeta() != null))
    }
  }

  test("create btree global index with multiple partitions") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, name STRING, pt STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |  PARTITIONED BY (pt)
                  |""".stripMargin)

      var values =
        (0 until 65000).map(i => s"($i, 'name_$i', 'p0')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 22222).map(i => s"($i, 'name_$i', 'p0')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 35000).map(i => s"($i, 'name_$i', 'p1')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 100).map(i => s"($i, 'name_$i', 'p1')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 33333).map(i => s"($i, 'name_$i', 'p1')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 100).map(i => s"($i, 'name_$i', 'p2')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 33333).map(i => s"($i, 'name_$i', 'p2')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output =
        spark
          .sql(
            "CALL sys.create_global_index(table => 'test.T', index_column => 'name', index_type => 'btree'," +
              " options => 'btree-index.records-per-range=1000')")
          .collect()
          .head

      assert(output.getBoolean(0))

      assertMultiplePartitionsResult("T", 189088L, 3)
    }
  }

  test("create btree index within one spark partition") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, name STRING, pt STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |  PARTITIONED BY (pt)
                  |""".stripMargin)

      var values =
        (0 until 65000).map(i => s"($i, 'name_$i', 'p0')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 35000).map(i => s"($i, 'name_$i', 'p1')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      // force output parallelism = 1
      val output =
        spark
          .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'name', index_type => 'btree'," +
            " options => 'btree-index.records-per-range=1000,btree-index.build.max-parallelism=1')")
          .collect()
          .head

      assert(output.getBoolean(0))

      assertMultiplePartitionsResult("T", 100000L, 2)
    }
  }

  private def assertMultiplePartitionsResult(
      tableName: String,
      rowCount: Long,
      partCount: Int
  ): Unit = {
    val table = loadTable(tableName)
    val btreeEntries = table
      .store()
      .newIndexFileHandler()
      .scanEntries()
      .asScala
      .filter(_.indexFile().indexType() == "btree")
    assert(btreeEntries.nonEmpty)

    // 1. assert total row count
    val totalRowCount = btreeEntries.map(_.indexFile().rowCount()).sum
    assert(totalRowCount == rowCount)

    // 2. assert global index meta not null
    btreeEntries.foreach(e => assert(e.indexFile().globalIndexMeta() != null))

    // 3. assert non-overlapped within each partition
    val entriesByPart = btreeEntries.groupBy(_.partition())
    assert(entriesByPart.size == partCount)

    case class MetaWithKey(meta: SortedIndexFileMeta, first: Object, last: Object)
    val keySerializer = KeySerializer.create(new VarCharType())
    val comparator = keySerializer.createComparator()

    def deserialize(bytes: Array[Byte]): Object = {
      keySerializer.deserialize(MemorySlice.wrap(bytes))
    }

    for ((k, v) <- entriesByPart) {
      val metas = v
        .map(_.indexFile().globalIndexMeta().indexMeta())
        .map(bytes => SortedIndexFileMeta.deserialize(bytes))
        .map(
          m => {
            assert(m.getFirstKey != null)
            assert(m.getLastKey != null)
            MetaWithKey(m, deserialize(m.getFirstKey), deserialize(m.getLastKey))
          })

      val sorted = metas.sortWith((m1, m2) => comparator.compare(m1.first, m2.first) < 0)

      // should not overlap
      sorted.sliding(2).foreach {
        case Seq(prev: MetaWithKey, next: MetaWithKey) =>
          assert(
            comparator.compare(prev.last, next.first) <= 0,
            s"Found overlap for partition ${k.getString(0).toString}. The last key ${prev.last}, next first key ${next.first}"
          )
        case _ => // ignore
      }
    }
  }

  private def createBTreeIndex(tableName: String, column: String): Unit = {
    spark
      .sql(
        s"CALL sys.create_global_index(table => 'test.$tableName', " +
          s"index_column => '$column', index_type => 'btree')")
      .collect()
  }
}
