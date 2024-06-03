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

import org.apache.paimon.Snapshot.CommitKind
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.optimizer.MergePaimonScalarSubqueriers

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, LogicalPlan, OneRowRelation, WithCTE}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.Assertions

import scala.collection.immutable

abstract class PaimonOptimizationTestBase extends PaimonSparkTestBase {

  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._
  import testImplicits._

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: immutable.Seq[Batch] =
      Batch("MergePaimonScalarSubqueries", Once, MergePaimonScalarSubqueriers) :: Nil
  }

  test("Paimon Optimization: merge scalar subqueries") {
    withTable("T") {

      spark.sql(s"""
                   |CREATE TABLE T (a INT, b DOUBLE, c STRING)
                   |""".stripMargin)

      spark.sql("INSERT INTO T values (1, 11.1, 'x1'), (2, 22.2, 'x2'), (3, 33.3, 'x3')")

      val query = spark.sql(s"""
                               |SELECT
                               |  (SELECT COUNT(1) AS cnt FROM T),
                               |  (SELECT SUM(a) AS sum_a FROM T),
                               |  (SELECT AVG(b) AS avg_b FROM T)
                               |""".stripMargin)
      val optimizedPlan = Optimize.execute(query.queryExecution.analyzed)

      val relation = createRelationV2("T")
      val mergedSubquery = relation
        .select(
          count(Literal(1)).as("cnt"),
          sum(col("a").expr).as("sum_a"),
          avg(col("b").expr).as("avg_b")
        )
        .select(
          CreateNamedStruct(
            Seq(
              Literal("cnt"),
              'cnt,
              Literal("sum_a"),
              'sum_a,
              Literal("avg_b"),
              'avg_b
            )).as("mergedValue"))
      val analyzedMergedSubquery = mergedSubquery.analyze
      val correctAnswer = WithCTE(
        OneRowRelation()
          .select(
            extractorExpression(0, analyzedMergedSubquery.output, 0),
            extractorExpression(0, analyzedMergedSubquery.output, 1),
            extractorExpression(0, analyzedMergedSubquery.output, 2)
          ),
        Seq(definitionNode(analyzedMergedSubquery, 0))
      )
      // Check the plan applied MergePaimonScalarSubqueries.
      comparePlans(optimizedPlan.analyze, correctAnswer.analyze)

      // Check the query's result.
      checkDataset(query.as[(Long, Long, Double)], (3L, 6L, 22.2))
    }
  }

  test("Paimon Optimization: paimon scan equals") {
    withTable("T") {
      spark.sql(s"CREATE TABLE T (id INT, name STRING, pt STRING) PARTITIONED BY (pt)")
      spark.sql(s"INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2')")

      val sqlText = "SELECT * FROM T WHERE id = 1 AND pt = 'p1' LIMIT 1"
      def getPaimonScan(sqlText: String) = {
        spark
          .sql(sqlText)
          .queryExecution
          .optimizedPlan
          .collectFirst { case relation: DataSourceV2ScanRelation => relation }
          .get
          .scan
      }
      Assertions.assertEquals(getPaimonScan(sqlText), getPaimonScan(sqlText))
    }
  }

  test(s"Paimon Optimization: eval subqueries for delete table with ScalarSubquery") {
    withPk.foreach(
      hasPk => {
        val tblProps = if (hasPk) {
          s"TBLPROPERTIES ('primary-key'='id, pt')"
        } else {
          ""
        }
        withTable("t1", "t2") {
          spark.sql(s"""
                       |CREATE TABLE t1 (id INT, name STRING, pt INT)
                       |$tblProps
                       |PARTITIONED BY (pt)
                       |""".stripMargin)
          spark.sql(
            "INSERT INTO t1 VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 2), (4, 'd', 3), (5, 'e', 4)")

          spark.sql(s"CREATE TABLE t2 (id INT, n INT)")
          spark.sql("INSERT INTO t2 VALUES (1, 1), (2, 2), (3, 3), (4, 4)")

          spark.sql(s"""DELETE FROM t1 WHERE
                       |pt >= (SELECT min(id) FROM t2 WHERE n BETWEEN 2 AND 3)
                       |AND
                       |pt <= (SELECT max(id) FROM t2 WHERE n BETWEEN 2 AND 3)""".stripMargin)
          // For partition-only predicates, drop partition is called internally.
          Assertions.assertEquals(
            CommitKind.OVERWRITE,
            loadTable("t1").store().snapshotManager().latestSnapshot().commitKind())

          checkAnswer(
            spark.sql("SELECT * FROM t1 ORDER BY id"),
            Row(1, "a", 1) :: Row(5, "e", 4) :: Nil)
        }
      })
  }

  test(s"Paimon Optimization: eval subqueries for delete table with InSubquery") {
    withPk.foreach(
      hasPk => {
        val tblProps = if (hasPk) {
          s"TBLPROPERTIES ('primary-key'='id, pt')"
        } else {
          ""
        }
        withTable("t1", "t2") {
          spark.sql(s"""
                       |CREATE TABLE t1 (id INT, name STRING, pt INT)
                       |$tblProps
                       |PARTITIONED BY (pt)
                       |""".stripMargin)
          spark.sql(
            "INSERT INTO t1 VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 2), (4, 'd', 3), (5, 'e', 4)")

          spark.sql(s"CREATE TABLE t2 (id INT, n INT)")
          spark.sql("INSERT INTO t2 VALUES (1, 1), (2, 2), (3, 3), (4, 4)")

          spark.sql(s"""DELETE FROM t1 WHERE
                       |pt in (SELECT id FROM t2 WHERE n BETWEEN 2 AND 3)
                       |OR
                       |pt in (SELECT max(id) FROM t2 WHERE n BETWEEN 2 AND 3)""".stripMargin)
          // For partition-only predicates, drop partition is called internally.
          Assertions.assertEquals(
            CommitKind.OVERWRITE,
            loadTable("t1").store().snapshotManager().latestSnapshot().commitKind())

          checkAnswer(
            spark.sql("SELECT * FROM t1 ORDER BY id"),
            Row(1, "a", 1) :: Row(5, "e", 4) :: Nil)
        }
      })
  }

  private def definitionNode(plan: LogicalPlan, cteIndex: Int) = {
    CTERelationDef(plan, cteIndex, underSubquery = true)
  }

  def extractorExpression(cteIndex: Int, output: Seq[Attribute], fieldIndex: Int): NamedExpression

}
