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

import org.apache.paimon.data.BinaryString
import org.apache.paimon.globalindex.{GlobalIndexScanner, KeySerializer, ReversedKeySerializer, SortedIndexFileMeta}
import org.apache.paimon.index.IndexFileMeta
import org.apache.paimon.memory.MemorySlice
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.VarCharType
import org.apache.paimon.utils.Range

import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.streaming.StreamTest

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.immutable

class CreateGlobalIndexProcedureTest extends PaimonSparkTestBase with StreamTest {

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

  test("create reverse-btree global index and query endsWith") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, name STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      // non-ASCII suffixes exercise multi-byte UTF-8 through the engine sort
      val suffixes = Seq("red", "grün", "中文", "gold")
      val numRows = 4000
      val values =
        (0 until numRows)
          .map(i => s"($i, '名_${i}_${suffixes(i % suffixes.length)}')")
          .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output =
        spark
          .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'name'," +
            " index_type => 'reverse-btree', options => 'sorted-index.records-per-range=200')")
          .collect()
          .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val reverseEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == "reverse-btree")
        .map(_.indexFile())
      assert(reverseEntries.nonEmpty)
      assert(reverseEntries.map(_.rowCount()).sum == numRows)

      // index files must be ordered and non-overlapping in the reversed key space
      val reversedSerializer = new ReversedKeySerializer(KeySerializer.create(new VarCharType()))
      val reversedComparator = reversedSerializer.createComparator()
      def deserialize(bytes: Array[Byte]): Object =
        reversedSerializer.deserialize(MemorySlice.wrap(bytes))
      val metas = reverseEntries
        .map(_.globalIndexMeta().indexMeta())
        .map(meta => SortedIndexFileMeta.deserialize(meta))
        .map(m => (deserialize(m.getFirstKey), deserialize(m.getLastKey)))
        .sortWith((a, b) => reversedComparator.compare(a._1, b._1) < 0)
      metas.sliding(2).foreach {
        case Seq(prev, next) => assert(reversedComparator.compare(prev._2, next._1) <= 0)
        case _ => // ignore
      }

      val nameIdx = table.rowType().getFieldIndex("name")
      val predicateBuilder = new PredicateBuilder(table.rowType())
      val scannerOpt =
        GlobalIndexScanner.create(
          table,
          new java.util.HashSet[IndexFileMeta](reverseEntries.asJava))
      assert(scannerOpt.isPresent)
      val scanner = scannerOpt.get()
      try {
        var union = Set.empty[Long]
        for (suffix <- suffixes) {
          val result =
            scanner.scan(predicateBuilder.endsWith(nameIdx, BinaryString.fromString(suffix)))
          assert(result.isPresent)
          val ids = {
            val buffer = scala.collection.mutable.ArrayBuffer[Long]()
            val it = result.get().results().iterator()
            while (it.hasNext) buffer += it.next()
            buffer
          }
          assert(ids.size == numRows / suffixes.length, s"wrong match count for suffix '$suffix'")
          union ++= ids
        }
        assert(union.size == numRows)

        val none =
          scanner.scan(predicateBuilder.endsWith(nameIdx, BinaryString.fromString("nosuchsuffix")))
        assert(none.isPresent)
        assert(none.get().results().isEmpty)
      } finally {
        scanner.close()
      }
    }
  }

  test("reverse-btree global index rejects non-string column") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, name STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)
      spark.sql("INSERT INTO T VALUES (1, 'a'), (2, 'b')")

      val e = intercept[Exception] {
        spark
          .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'id'," +
            " index_type => 'reverse-btree')")
          .collect()
      }
      def messages(t: Throwable): String = {
        var result = ""
        var current: Throwable = t
        while (current != null) {
          result += current.getMessage + " "
          current = current.getCause
        }
        result
      }
      assert(messages(e).contains("only supports CHAR/VARCHAR"))
    }
  }
}
