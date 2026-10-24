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

import org.apache.paimon.spark.{PaimonSparkTestBase, SparkConnectorOptions}

import org.apache.spark.sql.{PaimonUtils, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, LessThan, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Discard, Instruction, Split}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec
import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.atomic.AtomicBoolean

abstract class MergeRowsCodegenTestBase extends PaimonSparkTestBase {

  import testImplicits._

  private val paimonCodegenKey =
    s"spark.paimon.${SparkConnectorOptions.MERGE_CODEGEN_ENABLED.key()}"

  protected def keepInstruction(condition: Expression, output: Seq[Expression]): Instruction

  test("merge row codegen requires Spark and Paimon flags") {
    assert(!SparkConnectorOptions.MERGE_CODEGEN_ENABLED.defaultValue())

    Seq(
      (false, true, false),
      (true, false, false),
      (true, true, true)
    ).foreach {
      case (paimonCodegenEnabled, sparkCodegenEnabled, expectedCodegen) =>
        withSparkSQLConf(
          paimonCodegenKey -> paimonCodegenEnabled.toString,
          "spark.sql.codegen.wholeStage" -> sparkCodegenEnabled.toString) {
          val input = Seq(
            (1, 10, true, true, 101L, "unused-1"),
            (2, 20, true, false, 102L, "unused-2"),
            (3, 30, false, true, 103L, "unused-3"),
            (4, 40, false, false, 104L, "unused-4"),
            (5, 50, true, true, 105L, "unused-5")
          ).toDF(
            "target_id",
            "source_value",
            "source_present",
            "target_present",
            MergeRows.ROW_ID,
            "unused")
          val inputPlan = input.queryExecution.analyzed
          val targetId = inputPlan.output(0)
          val sourceValue = inputPlan.output(1)
          val sourcePresent = inputPlan.output(2)
          val targetPresent = inputPlan.output(3)
          val output = Seq(
            AttributeReference("id", IntegerType, nullable = false)(),
            AttributeReference("value", IntegerType, nullable = false)())
          val mergeRows = MergeRows(
            isSourceRowPresent = sourcePresent,
            isTargetRowPresent = targetPresent,
            matchedInstructions = Seq(
              keepInstruction(LessThan(sourceValue, Literal(15)), Seq(targetId, sourceValue)),
              Discard(TrueLiteral)),
            notMatchedInstructions =
              Seq(keepInstruction(TrueLiteral, Seq(sourceValue, sourceValue))),
            notMatchedBySourceInstructions =
              Seq(Split(TrueLiteral, Seq(targetId, Literal(-1)), Seq(targetId, Literal(-2)))),
            checkCardinality = true,
            output = output,
            child = inputPlan
          )
          val result = PaimonUtils.createDataset(spark, mergeRows)
          val executedPlan = result.queryExecution.executedPlan
          val mergeRowsIsCodegen = containsMergeRowsCodegen(executedPlan)

          assert(mergeRowsIsCodegen == expectedCodegen, executedPlan)
          checkAnswer(result, Seq(Row(1, 10), Row(20, 20), Row(3, -1), Row(3, -2)))
        }
    }
  }

  test("Paimon merge query uses codegen") {
    withSparkSQLConf(paimonCodegenKey -> "true", "spark.sql.codegen.wholeStage" -> "true") {
      withTable("target") {
        sql("""
              |CREATE TABLE target (id INT, v INT)
              |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')
              |""".stripMargin)
        sql("INSERT INTO target VALUES (1, 1), (2, 2)")

        val mergeCodegenExecuted = new AtomicBoolean(false)
        val listener = new QueryExecutionListener {
          override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
            if (containsMergeRowsCodegen(qe.executedPlan)) {
              mergeCodegenExecuted.set(true)
            }
          }

          override def onFailure(
              funcName: String,
              qe: QueryExecution,
              exception: Exception): Unit = {}
        }

        spark.listenerManager.register(listener)
        try {
          sql("""
                |MERGE INTO target
                |USING (SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS source(id, v)) source
                |ON target.id = source.id
                |WHEN MATCHED AND source.id = 1 THEN UPDATE SET target.v = source.v
                |WHEN MATCHED AND source.id = 2 THEN DELETE
                |WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v)
                |""".stripMargin)
          Utils.waitUntilEventEmpty(spark)
        } finally {
          spark.listenerManager.unregister(listener)
        }

        assert(mergeCodegenExecuted.get(), "Expected Paimon MERGE INTO to use whole-stage codegen.")
        checkAnswer(sql("SELECT id, v FROM target ORDER BY id"), Seq(Row(1, 10), Row(3, 30)))
      }
    }
  }

  private def containsMergeRowsCodegen(plan: SparkPlan): Boolean = {
    plan.collectFirst {
      case stage: WholeStageCodegenExec
          if stage.collectFirst { case _: MergeRowsExec => true }.isDefined =>
        true
    }.isDefined
  }
}
