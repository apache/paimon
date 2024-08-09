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

import org.apache.paimon.spark.PaimonMetrics.{RESULTED_TABLE_FILES, SKIPPED_TABLE_FILES}
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.junit.jupiter.api.Assertions

class PaimonMetricTest extends PaimonSparkTestBase {

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

      def checkMetrics(s: String, skippedTableFiles: Long, resultedTableFiles: Long): Unit = {
        val scan = getPaimonScan(s)
        // call getInputPartitions to trigger scan
        scan.getInputPartitions
        val metrics = scan.reportDriverMetrics()
        Assertions.assertEquals(skippedTableFiles, metric(metrics, SKIPPED_TABLE_FILES))
        Assertions.assertEquals(resultedTableFiles, metric(metrics, RESULTED_TABLE_FILES))
      }

      checkMetrics(s"SELECT * FROM T", 0, 5)
      checkMetrics(s"SELECT * FROM T WHERE pt = 'p2'", 2, 3)

      sql(s"DELETE FROM T WHERE pt = 'p1'")
      checkMetrics(s"SELECT * FROM T", 0, 4)

      sql("CALL sys.compact(table => 'T', partitions => 'pt=\"p2\"')")
      checkMetrics(s"SELECT * FROM T", 0, 2)
      checkMetrics(s"SELECT * FROM T WHERE pt = 'p2'", 1, 1)
    }
  }

  test("Paimon Metric: report output metric") {
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

  def metric(metrics: Array[CustomTaskMetric], name: String): Long = {
    metrics.find(_.name() == name).get.value()
  }
}
