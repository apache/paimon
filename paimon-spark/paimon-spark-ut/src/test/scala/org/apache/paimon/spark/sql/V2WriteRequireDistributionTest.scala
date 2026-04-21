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
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, AppendDataExecV1}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

class V2WriteRequireDistributionTest extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {

  test("V2 write primary key table") {

    withTable("t1") {
      spark.sql(
        "CREATE TABLE t1 (type STRING, id INT, data STRING) partitioned by (type) TBLPROPERTIES ('primary-key' = 'type,id', 'bucket'='2', 'file.format'='avro')")
      val query =
        "INSERT INTO t1 VALUES ('foo', 1, 'x1'), ('foo',1, 'X1'), ('foo', 2, 'x2'), ('foo', 2, 'X2'), ('bar', 3, 'x3'), ('bar', 3, 'X3')"

      withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
        val df = spark.sql(query)
        val nodes = collect(
          df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan) {
          case shuffle: ShuffleExchangeLike => shuffle
          case append: AppendDataExec => append
        }

        assert(nodes.size == 2)

        val node1 = nodes(0)
        assert(
          node1.isInstanceOf[AppendDataExec] &&
            node1.toString.contains("PaimonWrite(table=paimon.test.t1"),
          s"Expected AppendDataExec with specific paimon write, but got: $node1"
        )

        val node2 = nodes(1)
        assert(
          node2.isInstanceOf[ShuffleExchangeLike] &&
            node2.toString.contains("org.apache.paimon.spark.catalog.functions.BucketFunction"),
          s"Expected ShuffleExchangeLike with BucketFunction, but got: $node2"
        )

        checkAnswer(
          spark.sql("SELECT * FROM t1"),
          Seq(
            Row("foo", 1, "X1"),
            Row("foo", 2, "X2"),
            Row("bar", 3, "X3")
          ))
      }
    }
  }

  test("V2 write append only table") {

    withTable("t1") {
      spark.sql(
        "CREATE TABLE t1 (type STRING, id INT, data STRING) partitioned by (type) TBLPROPERTIES ('bucket-key' = 'id', 'bucket'='2', 'file.format'='avro')")
      val query =
        "INSERT INTO t1 VALUES ('foo', 1, 'x1'), ('foo',1, 'X1'), ('foo', 2, 'x2'), ('foo', 2, 'X2'), ('bar', 3, 'x3'), ('bar', 3, 'X3')"

      withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
        val df = spark.sql(query)
        val nodes = collect(
          df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan) {
          case shuffle: ShuffleExchangeLike => shuffle
          case append: AppendDataExec => append
        }

        assert(nodes.size == 2)

        val node1 = nodes(0)
        assert(
          node1.isInstanceOf[AppendDataExec] &&
            node1.toString.contains("PaimonWrite(table=paimon.test.t1"),
          s"Expected AppendDataExec with specific paimon write, but got: $node1"
        )

        val node2 = nodes(1)
        assert(
          node2.isInstanceOf[ShuffleExchangeLike] &&
            node2.toString.contains("org.apache.paimon.spark.catalog.functions.BucketFunction"),
          s"Expected ShuffleExchangeLike with BucketFunction, but got: $node2"
        )
      }

      checkAnswer(
        spark.sql("SELECT * FROM t1"),
        Seq(
          Row("foo", 1, "x1"),
          Row("foo", 1, "X1"),
          Row("foo", 2, "x2"),
          Row("foo", 2, "X2"),
          Row("bar", 3, "x3"),
          Row("bar", 3, "X3")
        ))
    }
  }

  test("Fallback to v1 write") {

    withTable("t1") {
      spark.sql(
        "CREATE TABLE t1 (id INT, data STRING) partitioned by(data) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='-1')")

      val query = "INSERT INTO t1 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')"

      withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
        val df = spark.sql(query)
        val nodes = collect(
          df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan) {
          case append: AppendDataExecV1 => append
        }

        assert(nodes.size == 1)
        val node1 = nodes(0)
        assert(
          node1.isInstanceOf[AppendDataExecV1] &&
            node1.toString.contains("AppendDataExecV1 PrimaryKeyFileStoreTable[paimon.test.t1]"),
          s"Expected AppendDataExec with specific paimon write, but got: $node1"
        )
      }
    }
  }
}
