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
import org.apache.paimon.spark.execution.LateralVectorSearchExec
import org.apache.paimon.spark.read.SparkEngineContext

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{DataType, IntegerType}

import java.util.concurrent.atomic.AtomicInteger

/** Driver-side distributed LATERAL vector search for primary-key and data-evolution tables. */
class DistributedLateralVectorSearchTest extends PaimonSparkTestBase {

  test("distributed lateral primary-key vector search keeps query order") {
    withTable("T") {
      spark.sql(
        s"""
           |CREATE TABLE T (id INT, embedding ARRAY<FLOAT>)
           |TBLPROPERTIES (
           |  'primary-key' = 'id',
           |  'bucket' = '4',
           |  'deletion-vectors.enabled' = 'true',
           |  'vector-field' = 'embedding',
           |  'field.embedding.vector-dim' = '2',
           |  'pk-vector.index.columns' = 'embedding',
           |  'fields.embedding.pk-vector.index.type' = '${TestVectorGlobalIndexerFactory.IDENTIFIER}',
           |  'fields.embedding.pk-vector.distance.metric' = 'l2',
           |  'test.vector.dimension' = '2',
           |  'test.vector.metric' = 'l2',
           |  'global-index.thread-num' = '1',
           |  'vector-search.distribute.enabled' = 'true')
           |""".stripMargin)
      val values = (1 to 32).map(id => s"($id, array($id.0f, 0.0f))").mkString(", ")
      spark.sql(s"INSERT INTO T VALUES $values")

      val bucketCount =
        spark.sql("SELECT COUNT(DISTINCT bucket) FROM `T$buckets`").collect().head.getLong(0)
      assert(
        bucketCount >= 2,
        s"expected enough buckets to pass the distribution threshold, got $bucketCount")

      assertDistributedSearch(
        """
          |SELECT q.id AS query_id, r.id AS result_id
          |FROM (SELECT * FROM T WHERE id IN (32, 1)) AS q,
          |LATERAL (
          |  SELECT id
          |  FROM vector_search('T', 'embedding', q.embedding, 1)
          |) AS r
          |ORDER BY query_id
          |""".stripMargin,
        Seq((1, 1), (32, 32)),
        queryCount = 2,
        batchSize = 256
      )
    }
  }

  test("distributed lateral data-evolution vector search keeps query order") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (id INT, embedding ARRAY<FLOAT>)
                   |TBLPROPERTIES (
                   |  'bucket' = '-1',
                   |  'row-tracking.enabled' = 'true',
                   |  'data-evolution.enabled' = 'true',
                   |  'global-index.row-count-per-shard' = '8',
                   |  'global-index.thread-num' = '1',
                   |  'vector-search.distribute.enabled' = 'true',
                   |  'test.vector.dimension' = '2',
                   |  'test.vector.metric' = 'l2')
                   |""".stripMargin)
      val values = (1 to 32).map(id => s"($id, array($id.0f, 0.0f))").mkString(", ")
      spark.sql(s"INSERT INTO T VALUES $values")
      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'embedding', " +
          s"index_type => '${TestVectorGlobalIndexerFactory.IDENTIFIER}')")
        .collect()

      assertDistributedSearch(
        """
          |SELECT q.id AS query_id, r.id AS result_id
          |FROM (SELECT * FROM T WHERE id IN (32, 1)) AS q,
          |LATERAL (
          |  SELECT id
          |  FROM vector_search('T', 'embedding', q.embedding, 1)
          |) AS r
          |ORDER BY query_id
          |""".stripMargin,
        Seq((1, 1), (32, 32)),
        queryCount = 2,
        batchSize = 256
      )
    }
  }

  test("distributed lateral search bounds the number of search jobs") {
    withTable("T") {
      createPrimaryKeyTable("'vector-search.lateral-join.batch-size' = '3'")
      insertRows(1 to 32)
      assertEnoughBuckets()
      val queryCount = 6
      val batchSize = 3
      val jobCount = countDistributedSearchJobs {
        val rows = spark
          .sql("""
                 |SELECT q.id AS query_id, r.id AS result_id
                 |FROM (SELECT * FROM T WHERE id IN (1, 2, 3, 30, 31, 32)) AS q,
                 |LATERAL (
                 |  SELECT id
                 |  FROM vector_search('T', 'embedding', q.embedding, 1)
                 |) AS r
                 |ORDER BY query_id
                 |""".stripMargin)
          .collect()
        assert(
          rows.map(row => (row.getInt(0), row.getInt(1))).toSeq ==
            Seq((1, 1), (2, 2), (3, 3), (30, 30), (31, 31), (32, 32)))
      }
      assert(jobCount >= 1, "expected a distributed search job")
      assert(
        jobCount <= LateralVectorSearchExec.maxDistributedSearchJobs(queryCount, batchSize),
        s"distributed search submitted $jobCount jobs, above the query and batch-size bound"
      )
    }
  }

  test("distributed lateral search rejects too many query vectors before searching") {
    withTable("T") {
      createPrimaryKeyTable("'vector-search.distribute.lateral-max-queries' = '1'")
      insertRows(1 to 32)
      assertEnoughBuckets()
      val jobCount = countDistributedSearchJobs {
        val caught = intercept[Exception] {
          spark
            .sql("""
                   |SELECT q.id AS query_id, r.id AS result_id
                   |FROM (SELECT * FROM T WHERE id IN (1, 32)) AS q,
                   |LATERAL (
                   |  SELECT id
                   |  FROM vector_search('T', 'embedding', q.embedding, 1)
                   |) AS r
                   |""".stripMargin)
            .collect()
        }
        val message = exceptionText(caught)
        assert(message.contains("vector-search.distribute.lateral-max-queries"))
        assert(message.contains("vector-search.distribute.enabled"))
        assert(message.contains("vector-search.lateral-join.batch-size"))
      }
      assert(
        jobCount == 0,
        s"search job was submitted after the query limit was exceeded: $jobCount")
    }
  }

  test("distributed lateral search rejects wide outer rows before searching") {
    withTable("T", "Q") {
      createPrimaryKeyTable("'vector-search.distribute.lateral-max-query-bytes' = '64 b'")
      insertRows(1 to 32)
      spark.sql("CREATE TABLE Q (id INT, embedding ARRAY<FLOAT>, payload STRING)")
      spark.sql("INSERT INTO Q VALUES (1, array(1.0f, 0.0f), repeat('x', 128))")
      val jobCount = countDistributedSearchJobs {
        val caught = intercept[Exception] {
          spark
            .sql("""
                   |SELECT q.id, q.payload, r.id
                   |FROM Q AS q,
                   |LATERAL (
                   |  SELECT id FROM vector_search('T', 'embedding', q.embedding, 1)
                   |) AS r
                   |""".stripMargin)
            .collect()
        }
        val message = exceptionText(caught)
        assert(message.contains("vector-search.distribute.lateral-max-query-bytes"))
        assert(message.contains("outer row and query vector"))
      }
      assert(jobCount == 0, s"search job was submitted for an oversized outer row: $jobCount")
    }
  }

  test("distributed lateral query byte limit includes the separate float vector") {
    withTable("T") {
      // The outer UnsafeRow takes 48 bytes, and its separate two-float vector takes another 8.
      createPrimaryKeyTable("'vector-search.distribute.lateral-max-query-bytes' = '48 b'")
      insertRows(1 to 32)
      assertEnoughBuckets()
      val query = """
                    |SELECT q.id AS query_id, r.id AS result_id
                    |FROM (SELECT id, embedding FROM T WHERE id = 1) AS q,
                    |LATERAL (
                    |  SELECT id FROM vector_search('T', 'embedding', q.embedding, 1)
                    |) AS r
                    |""".stripMargin
      val jobCount = countDistributedSearchJobs {
        val caught = intercept[Exception](spark.sql(query).collect())
        assert(exceptionText(caught).contains("outer row and query vector require 56 bytes"))
      }
      assert(jobCount == 0, s"search job was submitted for an oversized query: $jobCount")
      spark.sql(
        "ALTER TABLE T SET TBLPROPERTIES " +
          "('vector-search.distribute.lateral-max-query-bytes' = '56 b')")
      assertDistributedSearch(query, Seq((1, 1)), queryCount = 1, batchSize = 256)
    }
  }

  test("distributed lateral topK keeps duplicate vectors associated with outer rows") {
    withTable("T", "Q") {
      createPrimaryKeyTable("'vector-search.lateral-join.batch-size' = '2'")
      spark.sql("ALTER TABLE T ADD COLUMN (payload STRING)")
      spark.sql(
        "INSERT INTO T SELECT id, array(cast(id AS FLOAT), 0.0f), " +
          "concat('item-', id) FROM range(1, 33)")
      spark.sql("CREATE TABLE Q (id INT, embedding ARRAY<FLOAT>)")
      spark.sql("""
                  |INSERT INTO Q VALUES (101, array(1.0f, 0.0f)),
                  |(102, array(1.0f, 0.0f)), (103, array(32.0f, 0.0f))
                  |""".stripMargin)
      assertEnoughBuckets()
      val jobCount = countDistributedSearchJobs {
        val rows = spark
          .sql("""
                 |SELECT q.id, r.id, r.payload, r.__paimon_search_score
                 |FROM Q AS q,
                 |LATERAL (
                 |  SELECT id, payload, __paimon_search_score
                 |  FROM vector_search('T', 'embedding', q.embedding, 2)
                 |) AS r
                 |ORDER BY q.id, r.id
                 |""".stripMargin)
          .collect()
        assert(
          rows.map(row => (row.getInt(0), row.getInt(1))).toSeq ==
            Seq((101, 1), (101, 2), (102, 1), (102, 2), (103, 31), (103, 32)))
        rows.foreach {
          row =>
            assert(row.getString(2) == s"item-${row.getInt(1)}")
            val queryId = if (row.getInt(0) == 103) 32 else 1
            val expectedScore = 1.0f / (1 + math.abs(queryId - row.getInt(1)))
            assert(math.abs(row.getFloat(3) - expectedScore) < 0.00001f)
        }
      }
      assert(jobCount >= 1, "expected a distributed search job")
      assert(jobCount <= LateralVectorSearchExec.maxDistributedSearchJobs(3, 2))
    }
  }

  test("distributed lateral search rejects topK above the configured limit before searching") {
    withTable("T") {
      createPrimaryKeyTable("'vector-search.distribute.lateral-max-limit' = '1'")
      insertRows(1 to 32)
      assertEnoughBuckets()
      val jobCount = countDistributedSearchJobs {
        val caught = intercept[Exception] {
          spark
            .sql("""
                   |SELECT q.id AS query_id, r.id AS result_id
                   |FROM (SELECT * FROM T WHERE id = 1) AS q,
                   |LATERAL (
                   |  SELECT id
                   |  FROM vector_search('T', 'embedding', q.embedding, 2)
                   |) AS r
                   |""".stripMargin)
            .collect()
        }
        val message = exceptionText(caught)
        assert(message.contains("vector-search.distribute.lateral-max-limit"))
        assert(message.contains("vector-search.distribute.enabled"))
      }
      assert(jobCount == 0, s"search job was submitted after topK was rejected: $jobCount")
    }
  }

  test("distributed lateral search stops when retained result bytes exceed the limit") {
    withTable("T") {
      createPrimaryKeyTable(
        "'vector-search.lateral-join.batch-size' = '2'",
        "'vector-search.distribute.lateral-max-result-bytes' = '1 b'")
      insertRows(1 to 32)
      assertEnoughBuckets()
      val jobCount = countDistributedSearchJobs {
        val caught = intercept[Exception] {
          spark
            .sql("""
                   |SELECT q.id AS query_id, r.id AS result_id
                   |FROM (SELECT * FROM T WHERE id IN (1, 2, 31, 32)) AS q,
                   |LATERAL (
                   |  SELECT id
                   |  FROM vector_search('T', 'embedding', q.embedding, 1)
                   |) AS r
                   |""".stripMargin)
            .collect()
        }
        val message = exceptionText(caught)
        assert(message.contains("vector-search.distribute.lateral-max-result-bytes"))
        assert(message.contains("No further search batch is submitted."))
      }
      assert(
        jobCount == 1,
        s"expected the first search batch to run and later batches to be skipped, got $jobCount jobs")
    }
  }

  test("distributed result collection stops within an overflowing batch") {
    val projection = UnsafeProjection.create(Array[DataType](IntegerType))
    val rowBytes = projection(InternalRow(1)).getSizeInBytes
    var consumed = 0
    var batches = 0
    val rows = Iterator(Seq(1, 2, 3), Seq(4)).flatMap {
      batch =>
        batches += 1
        batch.iterator.map {
          value =>
            consumed += 1
            projection(InternalRow(value))
        }
    }
    val caught = intercept[IllegalArgumentException] {
      LateralVectorSearchExec.collectDistributedResults(rows, rowBytes.toLong)
    }
    assert(caught.getMessage.contains("vector-search.distribute.lateral-max-result-bytes"))
    assert(
      consumed == 2,
      "must stop on the overflowing row, without consuming the rest of the batch")
    assert(batches == 1, "must not start another batch")
  }

  test("distributed result collection accepts the byte boundary and copies reused rows") {
    val projection = UnsafeProjection.create(Array[DataType](IntegerType))
    val rowBytes = projection(InternalRow(1)).getSizeInBytes
    val rows = Iterator(1, 2).map(value => projection(InternalRow(value)))
    val result = LateralVectorSearchExec.collectDistributedResults(rows, rowBytes.toLong * 2)
    assert(result.map(_.length).sum == rowBytes * 2)
    assert(result.map {
      bytes =>
        val row = new UnsafeRow(1)
        row.pointTo(bytes, bytes.length)
        row.getInt(0)
    } == Seq(1, 2))
  }

  private def assertDistributedSearch(
      query: String,
      expected: Seq[(Int, Int)],
      queryCount: Int,
      batchSize: Int): Unit = {
    val jobCount = countDistributedSearchJobs {
      val rows = spark.sql(query).collect()
      assert(rows.map(row => (row.getInt(0), row.getInt(1))).toSeq == expected)
    }
    assert(jobCount >= 1, "LATERAL vector search did not submit a distributed Spark job")
    assert(
      jobCount <= LateralVectorSearchExec.maxDistributedSearchJobs(queryCount, batchSize),
      s"distributed search submitted $jobCount jobs, above the query and batch-size bound"
    )
  }

  private def countDistributedSearchJobs(body: => Unit): Int = {
    val jobs = new AtomicInteger()
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        val description = jobStart.properties.getProperty("spark.job.description")
        if (SparkEngineContext.DISTRIBUTED_VECTOR_SEARCH_JOB_DESCRIPTION == description) {
          jobs.incrementAndGet()
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      body
    } finally {
      try {
        flushSparkListeners()
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }
    jobs.get()
  }

  // listenerBus is private[spark], but the bytecode method is public.
  private def flushSparkListeners(): Unit = {
    val listenerBus =
      spark.sparkContext.getClass.getMethod("listenerBus").invoke(spark.sparkContext)
    listenerBus.getClass
      .getMethod("waitUntilEmpty", java.lang.Long.TYPE)
      .invoke(listenerBus, java.lang.Long.valueOf(30000L))
  }

  private def createPrimaryKeyTable(extraProperties: String*): Unit = {
    val properties = Seq(
      "'primary-key' = 'id'",
      "'bucket' = '4'",
      "'deletion-vectors.enabled' = 'true'",
      "'vector-field' = 'embedding'",
      "'field.embedding.vector-dim' = '2'",
      "'pk-vector.index.columns' = 'embedding'",
      s"'fields.embedding.pk-vector.index.type' = '${TestVectorGlobalIndexerFactory.IDENTIFIER}'",
      "'fields.embedding.pk-vector.distance.metric' = 'l2'",
      "'test.vector.dimension' = '2'",
      "'test.vector.metric' = 'l2'",
      "'global-index.thread-num' = '1'",
      "'vector-search.distribute.enabled' = 'true'"
    ) ++ extraProperties
    spark.sql(s"""
                 |CREATE TABLE T (id INT, embedding ARRAY<FLOAT>)
                 |TBLPROPERTIES (
                 |  ${properties.mkString(",\n")}
                 |)
                 |""".stripMargin)
  }

  private def assertEnoughBuckets(): Unit = {
    val bucketCount =
      spark.sql("SELECT COUNT(DISTINCT bucket) FROM `T$buckets`").collect().head.getLong(0)
    assert(
      bucketCount >= 2,
      s"expected enough buckets to pass the distribution threshold, got $bucketCount")
  }

  private def insertRows(ids: Range): Unit = {
    val values = ids.map(id => s"($id, array($id.0f, 0.0f))").mkString(", ")
    spark.sql(s"INSERT INTO T VALUES $values")
  }

  private def exceptionText(throwable: Throwable): String = {
    val text = new StringBuilder
    var current: Throwable = throwable
    while (current != null) {
      if (current.getMessage != null) {
        text.append(current.getMessage).append('\n')
      }
      current = current.getCause
    }
    text.toString()
  }
}
