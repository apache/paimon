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
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

class PushDownAggregatesTest extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {

  private def runAndCheckAggregate(
      query: String,
      expectedRows: Seq[Row],
      expectedNumAggregates: Int): Unit = {
    val df = spark.sql(query)
    checkAnswer(df, expectedRows)
    assert(df.schema.names.toSeq == df.queryExecution.executedPlan.output.map(_.name))
    assert(df.queryExecution.analyzed.find(_.isInstanceOf[Aggregate]).isDefined)
    val numAggregates = collect(df.queryExecution.executedPlan) {
      case agg: BaseAggregateExec => agg
    }.size
    assert(numAggregates == expectedNumAggregates, query)
    if (numAggregates == 0) {
      assert(collect(df.queryExecution.executedPlan) {
        case scan: LocalTableScanExec => scan
        // For compatibility with Spark3.x
        case e if e.getClass.getName == "org.apache.spark.sql.execution.EmptyRelationExec" => e
      }.size == 1)
    }
  }

  test("Push down aggregate - append table") {
    withTable("T") {
      spark.sql("CREATE TABLE T (c1 INT, c2 STRING) PARTITIONED BY(day STRING)")

      runAndCheckAggregate("SELECT COUNT(*) FROM T", Row(0) :: Nil, 0)
      // This query does not contain aggregate due to AQE optimize it to empty relation.
      runAndCheckAggregate("SELECT COUNT(*) FROM T GROUP BY c1", Nil, 0)
      runAndCheckAggregate("SELECT COUNT(c1) FROM T", Row(0) :: Nil, 2)
      runAndCheckAggregate("SELECT COUNT(*), COUNT(c1) FROM T", Row(0, 0) :: Nil, 2)
      runAndCheckAggregate("SELECT COUNT(*), COUNT(*) + 1 FROM T", Row(0, 1) :: Nil, 0)
      runAndCheckAggregate("SELECT COUNT(*) as c FROM T WHERE day='a'", Row(0) :: Nil, 0)
      runAndCheckAggregate("SELECT COUNT(*) FROM T WHERE c1=1", Row(0) :: Nil, 2)
      runAndCheckAggregate("SELECT COUNT(*) FROM T WHERE day='a' and c1=1", Row(0) :: Nil, 2)

      spark.sql(
        "INSERT INTO T VALUES(1, 'x', 'a'), (2, 'x', 'a'), (3, 'x', 'b'), (3, 'x', 'c'), (null, 'x', 'a')")

      runAndCheckAggregate("SELECT COUNT(*) FROM T", Row(5) :: Nil, 0)
      runAndCheckAggregate(
        "SELECT COUNT(*) FROM T GROUP BY c1",
        Row(1) :: Row(1) :: Row(1) :: Row(2) :: Nil,
        2)
      runAndCheckAggregate("SELECT COUNT(c1) FROM T", Row(4) :: Nil, 2)
      runAndCheckAggregate("SELECT COUNT(*), COUNT(c1) FROM T", Row(5, 4) :: Nil, 2)
      runAndCheckAggregate("SELECT COUNT(*), COUNT(*) + 1 FROM T", Row(5, 6) :: Nil, 0)
      runAndCheckAggregate("SELECT COUNT(*) as c FROM T WHERE day='a'", Row(3) :: Nil, 0)
      runAndCheckAggregate("SELECT COUNT(*) FROM T WHERE c1=1", Row(1) :: Nil, 2)
      runAndCheckAggregate("SELECT COUNT(*) FROM T WHERE day='a' and c1=1", Row(1) :: Nil, 2)
    }
  }

  test("Push down aggregate - group by partition column") {
    withTable("T") {
      spark.sql("CREATE TABLE T (c1 INT) PARTITIONED BY(day STRING, hour INT)")

      runAndCheckAggregate("SELECT COUNT(*) FROM T GROUP BY day", Nil, 0)
      runAndCheckAggregate("SELECT day, COUNT(*) as c FROM T GROUP BY day, hour", Nil, 0)
      runAndCheckAggregate("SELECT day, COUNT(*), hour FROM T GROUP BY day, hour", Nil, 0)
      runAndCheckAggregate(
        "SELECT day, COUNT(*), hour FROM T WHERE day='x' GROUP BY day, hour",
        Nil,
        0)
      // This query does not contain aggregate due to AQE optimize it to empty relation.
      runAndCheckAggregate("SELECT day, COUNT(*) FROM T GROUP BY c1, day", Nil, 0)

      spark.sql(
        "INSERT INTO T VALUES(1, 'x', 1), (2, 'x', 1), (3, 'x', 2), (3, 'x', 3), (null, 'y', null)")

      runAndCheckAggregate("SELECT COUNT(*) FROM T GROUP BY day", Row(1) :: Row(4) :: Nil, 0)
      runAndCheckAggregate(
        "SELECT day, COUNT(*) as c FROM T GROUP BY day, hour",
        Row("x", 1) :: Row("x", 1) :: Row("x", 2) :: Row("y", 1) :: Nil,
        0)
      runAndCheckAggregate(
        "SELECT day, COUNT(*), hour FROM T GROUP BY day, hour",
        Row("x", 1, 2) :: Row("y", 1, null) :: Row("x", 2, 1) :: Row("x", 1, 3) :: Nil,
        0)
      runAndCheckAggregate(
        "SELECT day, COUNT(*), hour FROM T WHERE day='x' GROUP BY day, hour",
        Row("x", 1, 2) :: Row("x", 1, 3) :: Row("x", 2, 1) :: Nil,
        0)
      runAndCheckAggregate(
        "SELECT day, COUNT(*) FROM T GROUP BY c1, day",
        Row("x", 1) :: Row("x", 1) :: Row("x", 2) :: Row("y", 1) :: Nil,
        2)
    }
  }

  test("Push down aggregate - primary key table with deletion vector") {
    Seq(true, false).foreach(
      deletionVectorsEnabled => {
        withTable("T") {
          spark.sql(s"""
                       |CREATE TABLE T (c1 INT, c2 STRING)
                       |TBLPROPERTIES (
                       |'primary-key' = 'c1',
                       |'deletion-vectors.enabled' = $deletionVectorsEnabled
                       |)
                       |""".stripMargin)
          runAndCheckAggregate("SELECT COUNT(*) FROM T", Row(0) :: Nil, 0)

          spark.sql("INSERT INTO T VALUES(1, 'x'), (2, 'x'), (3, 'x'), (3, 'x')")
          runAndCheckAggregate("SELECT COUNT(*) FROM T", Row(3) :: Nil, 0)

          spark.sql("INSERT INTO T VALUES(1, 'x_1')")
          if (deletionVectorsEnabled) {
            runAndCheckAggregate("SELECT COUNT(*) FROM T", Row(3) :: Nil, 0)
          } else {
            runAndCheckAggregate("SELECT COUNT(*) FROM T", Row(3) :: Nil, 2)
          }
        }
      })
  }

  test("Push down aggregate - table with deletion vector") {
    Seq(true, false).foreach(
      deletionVectorsEnabled => {
        Seq(true, false).foreach(
          primaryKeyTable => {
            withTable("T") {
              sql(s"""
                     |CREATE TABLE T (id INT)
                     |TBLPROPERTIES (
                     | 'deletion-vectors.enabled' = $deletionVectorsEnabled,
                     | '${if (primaryKeyTable) "primary-key" else "bucket-key"}' = 'id',
                     | 'bucket' = '1'
                     |)
                     |""".stripMargin)

              sql("INSERT INTO T SELECT id FROM range (0, 5000)")
              runAndCheckAggregate("SELECT COUNT(*) FROM T", Seq(Row(5000)), 0)

              sql("DELETE FROM T WHERE id > 100 and id <= 400")
              if (deletionVectorsEnabled || !primaryKeyTable) {
                runAndCheckAggregate("SELECT COUNT(*) FROM T", Seq(Row(4700)), 0)
              } else {
                runAndCheckAggregate("SELECT COUNT(*) FROM T", Seq(Row(4700)), 2)
              }
            }
          })
      })
  }
}
