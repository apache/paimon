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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.globalindex.ScoredGlobalIndexResult
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.schema.Schema
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.DataTypes

/** End-to-end tests for row-ranked multivector search. */
class MultiVectorSearchTest extends PaimonSparkTestBase {

  test("reject an explicit non-cosine metric when creating a list index on an empty table") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (chunks ARRAY<ARRAY<FLOAT>>)
                  |TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',
                  |'data-evolution.enabled'='true')""".stripMargin)
      val error = intercept[RuntimeException] {
        spark
          .sql(
            """CALL sys.create_global_index(table => 'test.T', index_column => 'chunks',
              |index_type => 'ivf-flat', options => 'ivf-flat.metric=inner_product')""".stripMargin)
          .collect()
      }
      assert(error.getCause.isInstanceOf[IllegalArgumentException])
      assert(
        error.getCause.getMessage.contains("require cosine similarity, got metric 'inner_product'"))
    }
  }

  test("reject matrix queries with unsupported SQL shapes during analysis") {
    withTable("T") {
      spark.sql("""CREATE TABLE T (id INT, chunks ARRAY<ARRAY<FLOAT>>)
                  |TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',
                  |'data-evolution.enabled'='true')""".stripMargin)
      val correlated = intercept[UnsupportedOperationException] {
        spark
          .sql("""SELECT r.id FROM T AS q, LATERAL (
                 |  SELECT id FROM vector_search('T', 'chunks', q.chunks, 1)
                 |) AS r""".stripMargin)
          .queryExecution
          .analyzed
      }
      assert(correlated.getMessage.contains("single query vector per outer row"))
      val integers = intercept[IllegalArgumentException] {
        spark
          .sql("SELECT id FROM vector_search('T', 'chunks', array(array(1, 2)), 1)")
          .queryExecution
          .analyzed
      }
      assert(integers.getMessage.contains("FLOAT or DOUBLE coordinates"))
    }
  }

  Seq("ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann").foreach {
    kind =>
      test(s"$kind range index ranks rows by MaxSim in Spark SQL") {
        withTable("T") {
          spark.sql("""CREATE TABLE T (id INT, chunks ARRAY<ARRAY<FLOAT>>)
                      |TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',
                      |'data-evolution.enabled'='true', 'vector-index.search-mode'='fast')
                      |""".stripMargin)
          // Rows 1 and 2 win the individual queries. Row 4 wins their combined score.
          // Row 5 supplies distinct training vectors for the quantized indexes.
          spark.sql("""INSERT INTO T SELECT cast(id AS INT), CASE id
                      |WHEN 1 THEN array_repeat(array(1f, 0f, 0f, 0f), 32)
                      |WHEN 2 THEN array(array(0f, 1f, 0f, 0f))
                      |WHEN 3 THEN array(array(1f, 1f, 0f, 0f))
                      |WHEN 4 THEN array(array(3f, 0f, 1f, 0f), array(0f, 4f, 1f, 0f))
                      |ELSE transform(sequence(1, 256), i ->
                      |  array(-1f, -1f, cast(i % 17 AS FLOAT), cast(i % 31 AS FLOAT)))
                      |END FROM range(1, 6, 1, 1)""".stripMargin)
          val query = """SELECT id, __paimon_search_score FROM vector_search('T', 'chunks',
                        |  array(array(1f, 0f, 0f, 0f), array(0f, 1f, 0f, 0f)), 2)
                        |ORDER BY __paimon_search_score DESC, id""".stripMargin
          assert(spark.sql(query).collect().isEmpty)

          val options = Seq(s"$kind.sharding=range", s"$kind.dimension=4") ++ (kind match {
            case "diskann" => Seq("diskann.pq.m=1")
            case "ivf-pq" => Seq("ivf-pq.nlist=1", "ivf-pq.pq.m=1")
            case _ => Seq(s"$kind.nlist=1")
          })
          spark
            .sql(s"""CALL sys.create_global_index(table => 'test.T', index_column => 'chunks',
                    |index_type => '$kind', options => '${options.mkString(",")}')""".stripMargin)
            .collect()
          val rows = spark.sql(query).collect()
          assert(rows.map(_.getInt(0)).toSeq == Seq(4, 3))
          assert(math.abs(rows(0).getFloat(1) - (3 / math.sqrt(10) + 4 / math.sqrt(17))) < 1e-5)
          assert(math.abs(rows(1).getFloat(1) - math.sqrt(2)) < 1e-5)
        }
      }
  }

  test("rank complete lists by MaxSim across indexed and uncovered rows") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, chunks ARRAY<ARRAY<FLOAT>>)
                  |TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',
                  |  'deletion-vectors.enabled'='true',
                  |  'data-evolution.enabled'='true', 'vector-index.search-mode'='detail')
                  |""".stripMargin)
      spark.sql("""
                  |INSERT INTO T VALUES
                  |(1, array_repeat(array(1f, 0f), 32)), (2, array(array(0f, 1f))),
                  |(3, array(null, array(1f, 1f))), (4, array()), (5, null)
                  |""".stripMargin)
      val matrix = "array(array(1f, 0f), array(0f, 1f))"
      def query(q: String, k: Int): Seq[(Int, Float)] =
        spark
          .sql(s"""SELECT id, __paimon_search_score FROM vector_search('T', 'chunks', $q, $k)
                  |ORDER BY __paimon_search_score DESC, id""".stripMargin)
          .collect()
          .map(row => (row.getInt(0), row.getFloat(1)))
          .toSeq

      val exact = query(matrix, 3)
      assert(exact.map(_._1) == Seq(3, 1, 2))
      assert(math.abs(exact.head._2 - math.sqrt(2)) < 1e-5)
      spark
        .sql("""CALL sys.create_global_index(table => 'test.T', index_column => 'chunks',
               |index_type => 'ivf-flat', options => 'ivf-flat.dimension=2,ivf-flat.nlist=1,
               |ivf-flat.distance.metric=cosine')""".stripMargin.replace("\n", ""))
        .collect()
      assert(query(matrix, 1) == exact.take(1))
      assert(query("array(1f, 0f)", 3).map(_._1) == Seq(1, 3, 2))
      val table = loadTable("T")
      val qs = Array(Array(1f, 0f), Array(0f, 1f))
      val predicate = new PredicateBuilder(table.rowType()).equal(0, Int.box(2))
      val filtered = table
        .newVectorSearchBuilder()
        .withVectorColumn("chunks")
        .withMultiVector(qs)
        .withLimit(1)
        .withFilter(predicate)
        .executeLocal()
        .asInstanceOf[ScoredGlobalIndexResult]
      assert(filtered.results().getLongCardinality == 1)
      assert(filtered.scoreGetter().score(filtered.results().toArray(1).head) == 1f)
      val batch = table
        .newBatchVectorSearchBuilder()
        .withVectorColumn("chunks")
        .withVectors(qs)
        .withLimit(1)
        .withOption("ivf.refine_factor", "2")
        .executeBatchLocal()
      assert(batch.size() == 2)
      assert(batch.get(0).results().toArray(1).head != batch.get(1).results().toArray(1).head)

      spark.sql("INSERT INTO T VALUES (6, array(array(1f, 0f), array(0f, 1f)))")
      assert(query(matrix, 2) == Seq((6, 2f), exact.head))
      spark.sql("DELETE FROM T WHERE id = 3")
      assert(query(matrix, 3).map(_._1) == Seq(6, 1, 2))
    }
  }

  test("two indexed list columns select their own dimensions in distributed search") {
    withTable("docs") {
      val schema = Schema
        .newBuilder()
        .column("id", DataTypes.INT())
        .column("chunks", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.FLOAT())))
        .column("images", DataTypes.ARRAY(DataTypes.VECTOR(3, DataTypes.FLOAT())))
        .option("bucket", "-1")
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true")
        .option("global-index.row-count-per-shard", "1")
        .option("global-index.thread-num", "1")
        .option("vector-search.distribute.enabled", "true")
        .build()
      paimonCatalog.createTable(Identifier.create(dbName0, "docs"), schema, false)
      spark.sql("""INSERT INTO docs VALUES
                  |(1, array(array(1f, 0f)), array(array(0f, 0f, 1f))),
                  |(2, array(array(0f, 1f)), array(array(1f, 0f, 0f)))""".stripMargin)
      Seq("chunks" -> "ivf-flat.dimension=2,", "images" -> "").foreach {
        case (column, dimension) =>
          spark
            .sql(s"""CALL sys.create_global_index(table => 'test.docs', index_column => '$column',
                    |index_type => 'ivf-flat', options => '${dimension}ivf-flat.nlist=1')
                    |""".stripMargin)
            .collect()
      }
      def winner(column: String, q: String): Int =
        spark
          .sql(s"SELECT id FROM vector_search('docs', '$column', array($q, $q), 1)")
          .collect()
          .head
          .getInt(0)
      assert(winner("chunks", "array(1f, 0f)") == 1)
      assert(winner("images", "array(1f, 0f, 0f)") == 2)
    }
  }
}
