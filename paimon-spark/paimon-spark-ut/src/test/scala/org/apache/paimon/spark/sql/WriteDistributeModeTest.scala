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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

class WriteDistributeModeTest extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {

  test("Write distribute mode: write partitioned bucket -1 table") {
    for (distributeMode <- Seq("none", "hash")) {
      withTable("t") {
        sql(
          "CREATE TABLE t (id INT, pt STRING) partitioned by (pt) TBLPROPERTIES ('file.format'='avro')")
        val query = "INSERT INTO t VALUES (1, 'p1'), (2, 'p2')"

        withSparkSQLConf(
          "spark.paimon.write.use-v2-write" -> "true",
          "spark.paimon.partition.sink-strategy" -> distributeMode) {
          val df = spark.sql(query)
          val shuffleNodes = collect(
            df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan) {
            case shuffle: ShuffleExchangeLike => shuffle
          }

          if (distributeMode == "none") {
            assert(shuffleNodes.isEmpty)
          } else {
            assert(shuffleNodes.size == 1)
          }

          checkAnswer(spark.sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, "p1"), Row(2, "p2")))
        }
      }
    }
  }
}
