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

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.write.PaimonBatchWrite

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.paimon.Utils

class WriteMetricsTest extends PaimonSparkTestBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")

  test("missing execution metrics") {
    assert(spark.sharedState.statusStore.execution(-1L).isEmpty)
    assert(Compatibility.getExecutionMetrics(spark, -1L).isEmpty)
  }

  test("empty dataframe append with and without execution metrics") {
    for (disableListener <- Seq(false, true)) {
      withTable("source", "target") {
        sql(
          "CREATE TABLE source (id INT, v INT) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')")
        sql(
          "CREATE TABLE target (id INT, v INT) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')")
        sql("INSERT INTO target VALUES (1, 10)")
        val before = loadTable("target").snapshotManager().latestSnapshotId()
        val source = spark.read.table("source").limit(0)
        val count = source.count()
        assert(count == 0)
        assert(source.queryExecution.executedPlan.isInstanceOf[LocalTableScanExec])
        def append(): Unit = {
          source
            .coalesce(math.max(1, (count / 10000).toInt))
            .write
            .format("paimon")
            .mode("append")
            .saveAsTable("target")
        }
        if (disableListener) {
          withoutStatusListener(append())
        } else {
          append()
        }
        checkAnswer(spark.table("target"), Row(1, 10) :: Nil)
        assert(loadTable("target").snapshotManager().latestSnapshotId() == before)
      }
    }
  }

  test("nonempty dataframe append without execution metrics") {
    import testImplicits._
    withTable("target") {
      sql(
        "CREATE TABLE target (id INT, v INT) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')")
      withoutStatusListener {
        Seq((1, 10), (2, 20))
          .toDF("id", "v")
          .write
          .format("paimon")
          .mode("append")
          .saveAsTable("target")
      }
      assert(loadTable("target").latestSnapshot().isPresent)
      checkAnswer(spark.table("target"), Seq(Row(1, 10), Row(2, 20)))
    }
  }

  test("nonempty batch commit without execution id") {
    withTable("target") {
      sql(
        "CREATE TABLE target (id INT, v INT) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')")
      val table = loadTable("target")
      val schema = spark.table("target").schema
      val batchWrite = PaimonBatchWrite(table, schema, schema, None, None)
      val writer = batchWrite.createBatchWriterFactory(null).createWriter(0, 0L)
      try {
        writer.write(new GenericInternalRow(Array[Any](1, 10)))
        val message = writer.commit()
        assert(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY) == null)
        batchWrite.commit(Array(message))
      } finally {
        writer.close()
      }
      assert(table.latestSnapshot().isPresent)
      checkAnswer(spark.table("target"), Row(1, 10) :: Nil)
    }
  }

  // Use the real SQL status store with no listener to deterministically cover an absent
  // execution record while Spark still supplies the write's actual execution id.
  private def withoutStatusListener(f: => Unit): Unit = {
    Utils.waitUntilEventEmpty(spark)
    val listener = spark.sharedState.statusStore.listener.get
    spark.sparkContext.removeSparkListener(listener)
    try f
    finally spark.sparkContext.addSparkListener(listener)
  }
}
