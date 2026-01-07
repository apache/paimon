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

import org.apache.paimon.spark.PaimonMetrics.{RESULTED_TABLE_FILES, SCANNED_SNAPSHOT_ID, SKIPPED_TABLE_FILES}
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.scan.PaimonSplitScan
import org.apache.paimon.spark.util.ScanPlanHelper
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.junit.jupiter.api.Assertions

class PaimonMetricTest extends PaimonSparkTestBase with ScanPlanHelper {

  test(s"Paimon Metric: scan driver metric") {
    // Spark support reportDriverMetrics since Spark 3.4
    if (gteqSpark3_4) {
      sql(s"""
             |CREATE TABLE T (id INT, name STRING, pt STRING)
             |TBLPROPERTIES ('bucket'='1', 'bucket-key'='id', 'write-only'='true')
             |PARTITIONED BY (pt)
             |""".stripMargin)

      sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
      sql(s"INSERT INTO T VALUES (3, 'c', 'p2'), (4, 'c', 'p3')")
      sql(s"INSERT INTO T VALUES (5, 'd', 'p2')")

      def checkMetrics(
          s: String,
          scannedSnapshotId: Long,
          skippedTableFiles: Long,
          resultedTableFiles: Long): Unit = {
        val scan = getPaimonScan(s)
        // call getInputPartitions to trigger scan
        scan.inputPartitions
        val metrics = scan.reportDriverMetrics()
        Assertions.assertEquals(scannedSnapshotId, metric(metrics, SCANNED_SNAPSHOT_ID))
        Assertions.assertEquals(skippedTableFiles, metric(metrics, SKIPPED_TABLE_FILES))
        Assertions.assertEquals(resultedTableFiles, metric(metrics, RESULTED_TABLE_FILES))
      }

      checkMetrics(s"SELECT * FROM T", 3, 0, 5)
      checkMetrics(s"SELECT * FROM T WHERE pt = 'p2'", 3, 2, 3)

      sql(s"DELETE FROM T WHERE pt = 'p1'")
      checkMetrics(s"SELECT * FROM T", 4, 0, 4)

      sql("CALL sys.compact(table => 'T', partitions => 'pt=\"p2\"')")
      checkMetrics(s"SELECT * FROM T", 5, 0, 2)
      checkMetrics(s"SELECT * FROM T WHERE pt = 'p2'", 5, 1, 1)
    }
  }

  test(s"Paimon Metric: split scan driver metric") {
    // Spark support reportDriverMetrics since Spark 3.4
    if (gteqSpark3_4) {
      sql("CREATE TABLE T (id INT, name STRING)")
      sql(s"INSERT INTO T VALUES (1, 'a'), (2, 'b')")
      sql(s"INSERT INTO T VALUES (3, 'c')")

      val splits = getPaimonScan("SELECT * FROM T").inputSplits.map(_.asInstanceOf[DataSplit])
      val df = createDataset(spark, createNewScanPlan(splits, createRelationV2("T")))
      val scan = df.queryExecution.optimizedPlan
        .collectFirst { case relation: DataSourceV2ScanRelation => relation }
        .get
        .scan
        .asInstanceOf[PaimonSplitScan]

      val metrics = scan.reportDriverMetrics()
      assert(metric(metrics, RESULTED_TABLE_FILES) == 3)
    }
  }

  test("Paimon Metric: report output metric") {
    for (useV2Write <- Seq("true", "false")) {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
        withTable("T") {
          sql(s"CREATE TABLE T (id int)")

          var recordsWritten = 0L
          var bytesWritten = 0L

          val listener = new SparkListener() {
            override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
              val outputMetrics = taskEnd.taskMetrics.outputMetrics
              recordsWritten += outputMetrics.recordsWritten
              bytesWritten += outputMetrics.bytesWritten
            }
          }

          try {
            spark.sparkContext.addSparkListener(listener)
            sql(s"INSERT INTO T VALUES 1, 2, 3")
          } finally {
            spark.sparkContext.removeSparkListener(listener)
          }

          Assertions.assertEquals(3, recordsWritten)
          Assertions.assertTrue(bytesWritten > 0)
        }
      }
    }
  }

  test(s"Paimon Metric: v2 write metric") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      sql("CREATE TABLE T (id INT, name STRING, pt STRING) PARTITIONED BY (pt)")
      val df = sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p2')")
      val metrics =
        df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan.metrics
      val statusStore = spark.sharedState.statusStore
      val lastExecId = statusStore.executionsList().last.executionId
      val executionMetrics = statusStore.executionMetrics(lastExecId)

      assert(executionMetrics(metrics("appendedTableFiles").id) == "2")
      assert(executionMetrics(metrics("appendedRecords").id) == "2")
      assert(executionMetrics(metrics("appendedChangelogFiles").id) == "0")
      assert(executionMetrics(metrics("partitionsWritten").id) == "2")
      assert(executionMetrics(metrics("bucketsWritten").id) == "2")
    }
  }

  test(s"Paimon Metric: v2 write metric with adaptive plan") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      sql("CREATE TABLE T (id INT, pt INT) PARTITIONED BY (pt)")
      val df = sql(s"INSERT INTO T SELECT /*+ REPARTITION(1) */ id, id FROM range(1, 10)")
      val metrics =
        df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan.metrics
      val statusStore = spark.sharedState.statusStore
      val lastExecId = statusStore.executionsList().last.executionId
      val executionMetrics = statusStore.executionMetrics(lastExecId)

      assert(executionMetrics(metrics("appendedTableFiles").id) == "9")
      assert(executionMetrics(metrics("appendedRecords").id) == "9")
      assert(executionMetrics(metrics("partitionsWritten").id) == "9")
      assert(executionMetrics(metrics("bucketsWritten").id) == "9")
    }
  }

  def metric(metrics: Array[CustomTaskMetric], name: String): Long = {
    metrics.find(_.name() == name).get.value()
  }
}
