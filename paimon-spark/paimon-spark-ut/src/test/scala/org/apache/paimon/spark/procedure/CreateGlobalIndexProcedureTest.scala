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

import org.apache.paimon.globalindex.btree.{BTreeIndexMeta, KeySerializer}
import org.apache.paimon.memory.MemorySlice
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.VarCharType
import org.apache.paimon.utils.Range

import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.streaming.StreamTest

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.immutable

class CreateGlobalIndexProcedureTest extends PaimonSparkTestBase with StreamTest {

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
        (0 until 100000).map(i => s"($i, 'name_$i')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output =
        spark
          .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'name', index_type => 'bitmap')")
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
      table.store().newGlobalIndexScanBuilder().shardList()
      assert(bitmapEntries.nonEmpty)
      val totalRowCount = bitmapEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 100000L)
    }
  }

  test("create bitmap global index with partition") {
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

      values = (0 until 22222).map(i => s"($i, 'name_$i', 'p0')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 100).map(i => s"($i, 'name_$i', 'p1')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 100).map(i => s"($i, 'name_$i', 'p2')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 33333).map(i => s"($i, 'name_$i', 'p2')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 33333).map(i => s"($i, 'name_$i', 'p1')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output =
        spark
          .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'name', index_type => 'bitmap')")
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
      assert(bitmapEntries.nonEmpty)

      val ranges = bitmapEntries
        .map(
          s =>
            new Range(
              s.indexFile().globalIndexMeta().rowRangeStart(),
              s.indexFile().globalIndexMeta().rowRangeEnd()))
        .toList
        .asJava
      val mergedRange = Range.sortAndMergeOverlap(ranges, true)
      assert(mergedRange.size() == 1)
      assert(mergedRange.get(0).equals(new Range(0, 189087)))
      val totalRowCount = bitmapEntries
        .map(
          x =>
            x.indexFile()
              .globalIndexMeta()
              .rowRangeEnd() - x.indexFile().globalIndexMeta().rowRangeStart() + 1)
        .sum
      assert(totalRowCount == 189088L)
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
      table.store().newGlobalIndexScanBuilder().shardList()
      assert(btreeEntries.nonEmpty)

      // 1. assert total row count and file count
      val totalRowCount = btreeEntries.map(_.rowCount()).sum
      assert(btreeEntries.size == 100)
      assert(totalRowCount == 100000L)

      // 2. assert global index meta not null
      btreeEntries.foreach(e => assert(e.globalIndexMeta() != null))

      // 3. assert btree index file range non-overlapping
      case class MetaWithKey(meta: BTreeIndexMeta, first: Object, last: Object)
      val keySerializer = KeySerializer.create(new VarCharType())
      val comparator = keySerializer.createComparator()

      def deserialize(bytes: Array[Byte]): Object = {
        keySerializer.deserialize(MemorySlice.wrap(bytes))
      }

      val btreeMetas = btreeEntries
        .map(_.globalIndexMeta().indexMeta())
        .map(meta => BTreeIndexMeta.deserialize(meta))
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

      values = (0 until 35000).map(i => s"($i, 'name_$i', 'p1')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 22222).map(i => s"($i, 'name_$i', 'p0')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 100).map(i => s"($i, 'name_$i', 'p1')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 100).map(i => s"($i, 'name_$i', 'p2')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 33333).map(i => s"($i, 'name_$i', 'p2')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 33333).map(i => s"($i, 'name_$i', 'p1')").mkString(",")
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

  test("create bitmap global index with external path") {
    withTable("T") {
      val tempIndexDir: File = Utils.createTempDir
      val indexPath = "file:" + tempIndexDir.toString
      spark.sql(s"""
                   |CREATE TABLE T (id INT, name STRING)
                   |TBLPROPERTIES (
                   |  'bucket' = '-1',
                   |  'global-index.row-count-per-shard' = '10000',
                   |  'global-index.external-path' = '$indexPath',
                   |  'row-tracking.enabled' = 'true',
                   |  'data-evolution.enabled' = 'true')
                   |""".stripMargin)

      val values =
        (0 until 100000).map(i => s"($i, 'name_$i')").mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output =
        spark
          .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'name', index_type => 'bitmap')")
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
      assert(bitmapEntries.nonEmpty)
      val totalRowCount = bitmapEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 100000L)
      for (entry <- bitmapEntries) {
        assert(
          entry
            .indexFile()
            .externalPath()
            .startsWith(indexPath + "/" + entry.indexFile().fileName()))
      }
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
    table.store().newGlobalIndexScanBuilder().shardList()
    assert(btreeEntries.nonEmpty)

    // 1. assert total row count
    val totalRowCount = btreeEntries.map(_.indexFile().rowCount()).sum
    assert(totalRowCount == rowCount)

    // 2. assert global index meta not null
    btreeEntries.foreach(e => assert(e.indexFile().globalIndexMeta() != null))

    // 3. assert non-overlapped within each partition
    val entriesByPart = btreeEntries.groupBy(_.partition())
    assert(entriesByPart.size == partCount)

    case class MetaWithKey(meta: BTreeIndexMeta, first: Object, last: Object)
    val keySerializer = KeySerializer.create(new VarCharType())
    val comparator = keySerializer.createComparator()

    def deserialize(bytes: Array[Byte]): Object = {
      keySerializer.deserialize(MemorySlice.wrap(bytes))
    }

    for ((k, v) <- entriesByPart) {
      val metas = v
        .map(_.indexFile().globalIndexMeta().indexMeta())
        .map(bytes => BTreeIndexMeta.deserialize(bytes))
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
}
