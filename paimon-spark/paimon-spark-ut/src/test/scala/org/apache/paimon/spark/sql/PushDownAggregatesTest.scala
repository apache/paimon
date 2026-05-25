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

import java.sql.Date

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

  test("Push down aggregate - append table without partitions") {
    withTable("T") {
      spark.sql("CREATE TABLE T (c1 INT, c2 STRING, c3 DOUBLE, c4 DATE)")

      runAndCheckAggregate("SELECT COUNT(*) FROM T", Row(0) :: Nil, 0)
      runAndCheckAggregate(
        "SELECT COUNT(*), MIN(c1), MIN(c3), MIN(c4) FROM T",
        Row(0, null, null, null) :: Nil,
        0)
      runAndCheckAggregate(
        "SELECT COUNT(*), MAX(c1), MAX(c3), MAX(c4) FROM T",
        Row(0, null, null, null) :: Nil,
        0)
      // count(c1) and min/max for string are not supported.
      runAndCheckAggregate("SELECT COUNT(c1) FROM T", Row(0) :: Nil, 2)
      runAndCheckAggregate("SELECT MIN(c2) FROM T", Row(null) :: Nil, 2)
      runAndCheckAggregate("SELECT MAX(c2) FROM T", Row(null) :: Nil, 2)

      // This query does not contain aggregate due to AQE optimize it to empty relation.
      runAndCheckAggregate("SELECT COUNT(*) FROM T GROUP BY c1", Nil, 0)
      runAndCheckAggregate("SELECT COUNT(*), COUNT(c1) FROM T", Row(0, 0) :: Nil, 2)
      runAndCheckAggregate(
        "SELECT COUNT(*) + 1, MIN(c1) * 10, MAX(c3) + 1.0 FROM T",
        Row(1, null, null) :: Nil,
        0)
      runAndCheckAggregate(
        "SELECT COUNT(*) as cnt, MIN(c4) as min_c4 FROM T",
        Row(0, null) :: Nil,
        0)
      // The cases with common data filters are not supported.
      runAndCheckAggregate("SELECT COUNT(*) FROM T WHERE c1 = 1", Row(0) :: Nil, 2)

      spark.sql(
        s"""
           |INSERT INTO T VALUES (1, 'xyz', 11.1, TO_DATE('2025-01-01', 'yyyy-MM-dd')),
           |(2, null, null, TO_DATE('2025-01-01', 'yyyy-MM-dd')), (3, 'abc', 33.3, null),
           |(3, 'abc', null, TO_DATE('2025-03-01', 'yyyy-MM-dd')), (null, 'abc', 44.4, TO_DATE('2025-03-01', 'yyyy-MM-dd'))
           |""".stripMargin)

      val date1 = Date.valueOf("2025-01-01")
      val date2 = Date.valueOf("2025-03-01")
      runAndCheckAggregate("SELECT COUNT(*) FROM T", Row(5) :: Nil, 0)
      runAndCheckAggregate(
        "SELECT COUNT(*) FROM T GROUP BY c1",
        Row(1) :: Row(1) :: Row(1) :: Row(2) :: Nil,
        2)
      runAndCheckAggregate("SELECT COUNT(c1) FROM T", Row(4) :: Nil, 2)

      runAndCheckAggregate("SELECT COUNT(*), MIN(c1), MAX(c1) FROM T", Row(5, 1, 3) :: Nil, 0)
      runAndCheckAggregate(
        "SELECT COUNT(*), MIN(c2), MAX(c2) FROM T",
        Row(5, "abc", "xyz") :: Nil,
        2)
      runAndCheckAggregate("SELECT COUNT(*), MIN(c3), MAX(c3) FROM T", Row(5, 11.1, 44.4) :: Nil, 0)
      runAndCheckAggregate(
        "SELECT COUNT(*), MIN(c4), MAX(c4) FROM T",
        Row(5, date1, date2) :: Nil,
        0)
      runAndCheckAggregate("SELECT COUNT(*), COUNT(c1) FROM T", Row(5, 4) :: Nil, 2)
      runAndCheckAggregate("SELECT COUNT(*), COUNT(*) + 1 FROM T", Row(5, 6) :: Nil, 0)
      runAndCheckAggregate(
        "SELECT COUNT(*) + 1, MIN(c1) * 10, MAX(c3) + 1.0 FROM T",
        Row(6, 10, 45.4) :: Nil,
        0)
      runAndCheckAggregate(
        "SELECT MIN(c3) as min, MAX(c4) as max FROM T",
        Row(11.1, date2) :: Nil,
        0)
      runAndCheckAggregate("SELECT COUNT(*), MIN(c3) FROM T WHERE c1 = 3", Row(2, 33.3) :: Nil, 2)
    }
  }

  test("Push down aggregate - append table with partitions") {
    withTable("T") {
      spark.sql("CREATE TABLE T (c1 INT, c2 LONG) PARTITIONED BY(day STRING, hour INT)")

      runAndCheckAggregate("SELECT COUNT(*) FROM T GROUP BY day", Nil, 0)
      runAndCheckAggregate(
        "SELECT day, hour, COUNT(*), MIN(c1), MIN(c1) FROM T GROUP BY day, hour",
        Nil,
        0)
      runAndCheckAggregate(
        "SELECT day, hour, COUNT(*), MIN(c2), MIN(c2) FROM T GROUP BY day, hour",
        Nil,
        0)
      runAndCheckAggregate(
        "SELECT day, COUNT(*), hour FROM T WHERE day= '2025-01-01' GROUP BY day, hour",
        Nil,
        0)
      // This query does not contain aggregate due to AQE optimize it to empty relation.
      runAndCheckAggregate("SELECT day, COUNT(*) FROM T GROUP BY c1, day", Nil, 0)

      spark.sql(
        """
          |INSERT INTO T VALUES(1, 100L, '2025-01-01', 1), (2, null, '2025-01-01', 1),
          |(3, 300L, '2025-03-01', 3), (3, 330L, '2025-03-01', 3), (null, 400L, '2025-03-01', null)
          |""".stripMargin)

      runAndCheckAggregate("SELECT COUNT(*) FROM T GROUP BY day", Row(2) :: Row(3) :: Nil, 0)
      runAndCheckAggregate(
        "SELECT day, hour, COUNT(*) as c FROM T GROUP BY day, hour",
        Row("2025-01-01", 1, 2) :: Row("2025-03-01", 3, 2) :: Row("2025-03-01", null, 1) :: Nil,
        0)
      runAndCheckAggregate(
        "SELECT day, COUNT(*), hour, MIN(c1), MAX(c1) FROM T GROUP BY day, hour",
        Row("2025-01-01", 2, 1, 1, 2) :: Row("2025-03-01", 2, 3, 3, 3) :: Row(
          "2025-03-01",
          1,
          null,
          null,
          null) :: Nil,
        0
      )
      runAndCheckAggregate(
        "SELECT hour, COUNT(*), MIN(c2) as min, MAX(c2) as max FROM T WHERE day='2025-03-01' GROUP BY day, hour",
        Row(3, 2, 300L, 330L) :: Row(null, 1, 400L, 400L) :: Nil,
        0
      )
      runAndCheckAggregate(
        "SELECT c1, day, COUNT(*) FROM T GROUP BY c1, day ORDER BY c1, day",
        Row(null, "2025-03-01", 1) :: Row(1, "2025-01-01", 1) :: Row(2, "2025-01-01", 1) :: Row(
          3,
          "2025-03-01",
          2) :: Nil,
        2
      )
    }
  }

  test("Push down aggregate - append table with dense statistics") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (c1 INT, c2 STRING, c3 DOUBLE, c4 DATE)
                  |TBLPROPERTIES('metadata.stats-mode' = 'none')
                  |""".stripMargin)
      spark.sql(
        s"""
           |INSERT INTO T VALUES (1, 'xyz', 11.1, TO_DATE('2025-01-01', 'yyyy-MM-dd')),
           |(2, null, null, TO_DATE('2025-01-01', 'yyyy-MM-dd')), (3, 'abc', 33.3, null),
           |(3, 'abc', null, TO_DATE('2025-03-01', 'yyyy-MM-dd')), (null, 'abc', 44.4, TO_DATE('2025-03-01', 'yyyy-MM-dd'))
           |""".stripMargin)

      val date1 = Date.valueOf("2025-01-01")
      val date2 = Date.valueOf("2025-03-01")
      runAndCheckAggregate("SELECT COUNT(*) FROM T", Row(5) :: Nil, 0)

      // for metadata.stats-mode = none, no available statistics.
      runAndCheckAggregate("SELECT COUNT(*), MIN(c1), MAX(c1) FROM T", Row(5, 1, 3) :: Nil, 2)
      runAndCheckAggregate(
        "SELECT COUNT(*), MIN(c2), MAX(c2) FROM T",
        Row(5, "abc", "xyz") :: Nil,
        2)
      runAndCheckAggregate("SELECT COUNT(*), MIN(c3), MAX(c3) FROM T", Row(5, 11.1, 44.4) :: Nil, 2)
      runAndCheckAggregate(
        "SELECT COUNT(*), MIN(c4), MAX(c4) FROM T",
        Row(5, date1, date2) :: Nil,
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
            // should not push down min max for primary key table
            runAndCheckAggregate("SELECT MIN(c1) FROM T", Row(1) :: Nil, 2)
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

  test("Push down aggregate: group by partial partition of a multi partition table") {
    sql(s"""
           |CREATE TABLE T (
           |c1 STRING,
           |c2 STRING,
           |c3 STRING,
           |c4 STRING,
           |c5 DATE)
           |PARTITIONED BY (c5, c1)
           |TBLPROPERTIES ('primary-key' = 'c5, c1, c3')
           |""".stripMargin)

    sql("INSERT INTO T VALUES ('t1', 'k1', 'v1', 'r1', '2025-01-01')")
    checkAnswer(
      sql("SELECT COUNT(*) FROM T GROUP BY c1"),
      Seq(Row(1))
    )
    checkAnswer(
      sql("SELECT c1, COUNT(*) FROM T GROUP BY c1"),
      Seq(Row("t1", 1))
    )
    checkAnswer(
      sql("SELECT COUNT(*), c1 FROM T GROUP BY c1"),
      Seq(Row(1, "t1"))
    )
  }

  // https://github.com/apache/paimon/issues/6610
  test("Push down aggregate: aggregate a column in one partition is all null and another is not") {
    withTable("T") {
      spark.sql("CREATE TABLE T (c1 INT, c2 LONG) PARTITIONED BY(day STRING)")

      spark.sql("INSERT INTO T VALUES (1, 2, '2025-11-10')")
      spark.sql("INSERT INTO T VALUES (null, 2, '2025-11-11')")

      runAndCheckAggregate("SELECT MIN(c1) FROM T", Row(1) :: Nil, 0)
      runAndCheckAggregate("SELECT MAX(c1) FROM T", Row(1) :: Nil, 0)
    }
  }
}
