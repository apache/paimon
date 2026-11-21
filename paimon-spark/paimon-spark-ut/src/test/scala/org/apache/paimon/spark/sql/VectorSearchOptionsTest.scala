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

import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory
import org.apache.paimon.predicate.{FullTextSearch, HybridSearch, HybridSearchRoute, VectorSearch}
import org.apache.paimon.spark.{PaimonScan, PaimonSparkTestBase}

import org.apache.spark.sql.types.StructType
import org.mockito.Mockito.{spy, when}

import java.util.{Arrays, Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** Tests for vector search query-time options. */
class VectorSearchOptionsTest extends PaimonSparkTestBase {

  Seq("vector", "full-text", "hybrid").foreach {
    kind =>
      test(s"$kind search reads rows from its ranking snapshot") {
        withTable("T") {
          spark.sql("""
                      |CREATE TABLE T (id INT, v ARRAY<FLOAT>, content STRING)
                      |TBLPROPERTIES (
                      |  'bucket' = '-1',
                      |  'row-tracking.enabled' = 'true',
                      |  'data-evolution.enabled' = 'true',
                      |  'test.vector.dimension' = '2',
                      |  'test.vector.metric' = 'l2')
                      |""".stripMargin)
          spark.sql("INSERT INTO T VALUES (1, array(1.0f, 0.0f), 'apache paimon')").collect()
          spark
            .sql(s"CALL sys.create_global_index(table => 'T', index_column => 'v', " +
              s"index_type => '${TestVectorGlobalIndexerFactory.IDENTIFIER}')")
            .collect()
          spark
            .sql("CALL sys.create_global_index(table => 'T', index_column => 'content', " +
              "index_type => 'full-text')")
            .collect()
          val table = loadTable("T")
          val rankedSnapshot = table.latestSnapshot().get()
          spark.sql("INSERT OVERWRITE T VALUES (9, array(0.0f, 1.0f), 'later content')").collect()

          // Ranking sees the earlier snapshot while a separate data scan sees the overwrite.
          val searchTable = spy(table)
          when(searchTable.latestSnapshot()).thenReturn(Optional.of(rankedSnapshot))
          val vector = Array(1.0f, 0.0f)
          val query = """{"match":{"column":"content","terms":"paimon"}}"""
          val scan = PaimonScan(
            searchTable,
            new StructType().add("id", "int"),
            Nil,
            Nil,
            None,
            None,
            pushedVectorSearch =
              Some(new VectorSearch(vector, 1, "v")).filter(_ => kind == "vector"),
            pushedHybridSearch = Some(
              new HybridSearch(
                Arrays.asList(
                  new HybridSearchRoute("v", vector, 1, 1.0f),
                  HybridSearchRoute.fullText("content", query, 1, 1.0f, Collections.emptyMap())),
                1,
                "rrf")).filter(_ => kind == "hybrid"),
            pushedFullTextSearch = Some(new FullTextSearch("content", query, 1))
              .filter(_ => kind == "full-text")
          )

          val ids = ListBuffer.empty[Int]
          val reader = scan.readBuilder.newRead().createReader(scan.inputSplits.toList.asJava)
          try reader.forEachRemaining(row => ids.append(row.getInt(0)))
          finally reader.close()
          assert(ids.toList == List(1))
        }
      }
  }

  test("vector search forwards query options to reader") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true',
                  |  'test.vector.dimension' = '2',
                  |  'test.vector.required-option.key' = 'ivf.nprobe',
                  |  'test.vector.required-option.value' = '16')
                  |""".stripMargin)

      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (0, array(1.0f, 0.0f)),
                  |  (1, array(0.0f, 1.0f))
                  |""".stripMargin)

      spark
        .sql(s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', " +
          s"index_type => '${TestVectorGlobalIndexerFactory.IDENTIFIER}')")
        .collect()

      intercept[Exception] {
        spark
          .sql("""
                 |SELECT id FROM vector_search('T', 'v', array(1.0f, 0.0f), 1)
                 |""".stripMargin)
          .collect()
      }

      val result = spark
        .sql("""
               |SELECT id FROM vector_search(
               |  'T', 'v', array(1.0f, 0.0f), 1, map('ivf.nprobe', '16'))
               |""".stripMargin)
        .collect()

      assert(result.length == 1)

      val scores = spark
        .sql("""
               |SELECT __paimon_search_score FROM vector_search(
               |  'T', 'v', array(1.0f, 0.0f), 1, map('ivf.nprobe', '16'))
               |""".stripMargin)
        .collect()
      assert(scores.length == 1)
      assert(!scores.head.isNullAt(0))
    }
  }
}
