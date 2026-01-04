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

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.Range

import org.apache.spark.sql.streaming.StreamTest

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
}
