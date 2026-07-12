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
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.read.{SparkPrimaryKeyVectorRead, SparkVectorSearchBuilderImpl}
import org.apache.paimon.table.source.DataSplit

import scala.collection.JavaConverters._

/** End-to-end tests for primary-key vector search through Spark SQL. */
class PrimaryKeyVectorSearchTest extends PaimonSparkTestBase {

  test("distributed primary-key vector search selects Spark reader") {
    withTable("T") {
      createVectorTable()

      val builder = new SparkVectorSearchBuilderImpl(loadTable("T"))
      builder
        .withVectorColumn("embedding")
        .withVector(Array(0.0f, 0.0f))
        .withLimit(1)

      assert(builder.newVectorRead().isInstanceOf[SparkPrimaryKeyVectorRead])
    }
  }

  test("distributed primary-key vector search evaluates buckets in Spark") {
    withTable("T") {
      createVectorTable(
        bucket = 2,
        extraOptions = Seq("global-index.thread-num" -> "1"))
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (1, array(1.0f, 0.0f)),
                  |  (2, array(2.0f, 0.0f)),
                  |  (3, array(3.0f, 0.0f)),
                  |  (4, array(4.0f, 0.0f))
                  |""".stripMargin)

      val builder = new SparkVectorSearchBuilderImpl(loadTable("T"))
      builder
        .withVectorColumn("embedding")
        .withVector(Array(0.0f, 0.0f))
        .withLimit(2)

      val jobGroup = s"primary-key-vector-${System.nanoTime()}"
      spark.sparkContext.setJobGroup(jobGroup, jobGroup)
      try {
        builder.newVectorRead().read(builder.newVectorScan().scan())
      } finally {
        spark.sparkContext.clearJobGroup()
      }

      assert(spark.sparkContext.statusTracker.getJobIdsForGroup(jobGroup).nonEmpty)
    }
  }

  test("primary-key vector search uses bucket-local indexes") {
    withTable("T") {
      createVectorTable()

      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (1, array(3.0f, 0.0f)),
                  |  (2, array(1.0f, 0.0f)),
                  |  (3, array(2.0f, 0.0f))
                  |""".stripMargin)

      withSparkSQLConf("spark.paimon.vector-search.distribute.enabled" -> "true") {
        val rows = spark
          .sql("""
                 |SELECT id, __paimon_search_score
                 |FROM vector_search('T', 'embedding', array(0.0f, 0.0f), 2)
                 |""".stripMargin)
          .collect()

        assert(rows.map(_.getInt(0)).toSet == Set(2, 3))
        assert(rows.forall(!_.isNullAt(1)))

        val scores = spark
          .sql("""
                 |SELECT __paimon_search_score
                 |FROM vector_search('T', 'embedding', array(0.0f, 0.0f), 2)
                 |""".stripMargin)
          .collect()
        assert(scores.length == 2)
        assert(scores.forall(!_.isNullAt(0)))
      }
    }
  }

  test("primary-key vector search merges top k across buckets") {
    withTable("T") {
      createVectorTable(
        bucket = 4,
        extraOptions = Seq("global-index.thread-num" -> "2"))
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (1, array(1.0f, 0.0f)),
                  |  (2, array(2.0f, 0.0f)),
                  |  (3, array(3.0f, 0.0f)),
                  |  (4, array(4.0f, 0.0f)),
                  |  (5, array(5.0f, 0.0f)),
                  |  (6, array(6.0f, 0.0f)),
                  |  (7, array(7.0f, 0.0f)),
                  |  (8, array(8.0f, 0.0f)),
                  |  (9, array(9.0f, 0.0f)),
                  |  (10, array(10.0f, 0.0f)),
                  |  (11, array(11.0f, 0.0f)),
                  |  (12, array(12.0f, 0.0f)),
                  |  (13, array(13.0f, 0.0f)),
                  |  (14, array(14.0f, 0.0f)),
                  |  (15, array(15.0f, 0.0f)),
                  |  (16, array(16.0f, 0.0f))
                  |""".stripMargin)

      val buckets = loadTable("T")
        .newReadBuilder()
        .newScan()
        .plan()
        .splits()
        .asScala
        .map(_.asInstanceOf[DataSplit].bucket())
        .toSet
      assert(buckets == Set(0, 1, 2, 3))

      withSparkSQLConf("spark.paimon.vector-search.distribute.enabled" -> "true") {
        val ids = spark
          .sql("""
                 |SELECT id
                 |FROM vector_search('T', 'embedding', array(0.0f, 0.0f), 3)
                 |""".stripMargin)
          .collect()
          .map(_.getInt(0))
          .toSet
        assert(ids == Set(1, 2, 3))
      }
    }
  }

  test("primary-key vector search prunes partitions before top k") {
    withTable("T") {
      createVectorTable(
        columns = "id INT, embedding ARRAY<FLOAT>, dt STRING",
        primaryKey = "id,dt",
        partitionedBy = Some("dt"))
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (1, array(1.0f, 0.0f), 'A'),
                  |  (2, array(2.0f, 0.0f), 'A'),
                  |  (3, array(0.1f, 0.0f), 'B')
                  |""".stripMargin)

      withSparkSQLConf("spark.paimon.vector-search.distribute.enabled" -> "true") {
        val ids = spark
          .sql("""
                 |SELECT id
                 |FROM vector_search('T', 'embedding', array(0.0f, 0.0f), 2)
                 |WHERE dt = 'A'
                 |""".stripMargin)
          .collect()
          .map(_.getInt(0))
          .toSet
        assert(ids == Set(1, 2))
      }
    }
  }

  test("deduplicate updates and deletes primary-key vector results") {
    withTable("T") {
      createVectorTable()
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (1, array(3.0f, 0.0f)),
                  |  (2, array(1.0f, 0.0f))
                  |""".stripMargin)
      spark.sql("INSERT INTO T VALUES (1, array(0.5f, 0.0f))")

      withSparkSQLConf("spark.paimon.vector-search.distribute.enabled" -> "true") {
        val updated = spark
          .sql("""
                 |SELECT id
                 |FROM vector_search('T', 'embedding', array(0.0f, 0.0f), 1)
                 |""".stripMargin)
          .collect()
        assert(updated.map(_.getInt(0)).toSeq == Seq(1))

        spark.sql("DELETE FROM T WHERE id = 1")

        val afterDelete = spark
          .sql("""
                 |SELECT id
                 |FROM vector_search('T', 'embedding', array(0.0f, 0.0f), 1)
                 |""".stripMargin)
          .collect()
        assert(afterDelete.map(_.getInt(0)).toSeq == Seq(2))
      }
    }
  }

  test("partial update completes rows before publishing vector results") {
    withTable("T") {
      createVectorTable(
        columns = "id INT, payload STRING, embedding ARRAY<FLOAT>",
        extraOptions =
          Seq("merge-engine" -> "partial-update", "deletion-vectors.merge-on-read" -> "false")
      )
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (1, 'keep', array(3.0f, 0.0f)),
                  |  (2, 'other', array(1.0f, 0.0f))
                  |""".stripMargin)
      spark.sql("INSERT INTO T (id, embedding) VALUES (1, array(0.5f, 0.0f))")

      withSparkSQLConf("spark.paimon.vector-search.distribute.enabled" -> "true") {
        val rows = spark
          .sql("""
                 |SELECT id, payload
                 |FROM vector_search('T', 'embedding', array(0.0f, 0.0f), 1)
                 |""".stripMargin)
          .collect()
        assert(rows.length == 1)
        assert(rows.head.getInt(0) == 1)
        assert(rows.head.getString(1) == "keep")
      }
    }
  }

  private def createVectorTable(
      columns: String = "id INT, embedding ARRAY<FLOAT>",
      primaryKey: String = "id",
      bucket: Int = 1,
      extraOptions: Seq[(String, String)] = Seq.empty,
      partitionedBy: Option[String] = None): Unit = {
    val properties = (Seq(
      "primary-key" -> primaryKey,
      "bucket" -> bucket.toString,
      "deletion-vectors.enabled" -> "true",
      "vector-field" -> "embedding",
      "field.embedding.vector-dim" -> "2",
      "pk-vector.index.columns" -> "embedding",
      "fields.embedding.pk-vector.index.type" -> TestVectorGlobalIndexerFactory.IDENTIFIER,
      "fields.embedding.pk-vector.distance.metric" -> "l2",
      "test.vector.dimension" -> "2",
      "test.vector.metric" -> "l2"
    ) ++ extraOptions)
      .map { case (key, value) => s"'$key' = '$value'" }
      .mkString(",\n")
    val partitioning = partitionedBy.map(column => s"PARTITIONED BY ($column)").getOrElse("")
    spark.sql(s"""
                 |CREATE TABLE T ($columns)
                 |$partitioning
                 |TBLPROPERTIES ($properties)
                 |""".stripMargin)
  }
}
