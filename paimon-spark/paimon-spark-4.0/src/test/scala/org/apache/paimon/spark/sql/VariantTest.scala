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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GetStructField
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.{StructType, VariantType}

class VariantTest extends VariantTestBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.variant.inferShreddingSchema", "false")
  }
}

class VariantInferShreddingTest extends VariantTestBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.variant.inferShreddingSchema", "true")
  }
}

/**
 * Spark 4-specific plan-shape tests for the PushDownVariantExtract optimizer rule.
 *
 * These tests verify the rewrite at the Catalyst expression level: VariantGet(col, Literal(path),
 * targetType) → GetStructField(col, ordinal)
 *
 * and confirm that the variant column's output type in the scan relation changes from VariantType
 * to StructType after the pushdown, which is the evidence that Parquet column pruning will kick in
 * at read time.
 */
class VariantPushDownPlanTest extends PaimonSparkTestBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.paimon.variant.inferShreddingSchema", "true")

  // Explicit 3-field shredding schema used across all tests in this class.
  private val shreddedSchema3: String =
    """{"type":"ROW","fields":[{"name":"v","type":{"type":"ROW","fields":[""" +
      """{"name":"age","type":"INT"},""" +
      """{"name":"city","type":"STRING"},""" +
      """{"name":"score","type":"DOUBLE"}""" +
      """]}}]}"""

  test("Paimon Variant: VariantGet is replaced by GetStructField in optimized plan") {
    sql(s"""
           |CREATE TABLE T (id INT, v VARIANT)
           |TBLPROPERTIES ('parquet.variant.shreddingSchema' = '$shreddedSchema3')
           |""".stripMargin)
    sql("""
          |INSERT INTO T VALUES
          | (1, parse_json('{"age":26,"city":"Beijing","score":9.5}')),
          | (2, parse_json('{"age":27,"city":"Hangzhou","score":8.0}'))
          |""".stripMargin)

    val q =
      "SELECT variant_get(v, '$.age', 'int'), variant_get(v, '$.score', 'double') FROM T"
    checkAnswer(sql(q), Seq(Row(26, 9.5d), Row(27, 8.0d)))

    val projectExprs = sql(q).queryExecution.optimizedPlan
      .collectFirst { case p: Project => p }
      .get
      .projectList

    // After pushdown, no VariantGet should remain in the top-level project list.
    assert(
      !projectExprs.exists(_.exists(_.isInstanceOf[VariantGet])),
      "VariantGet should have been replaced by GetStructField after PushDownVariantExtract")

    // GetStructField nodes must now be present in its place.
    assert(
      projectExprs.exists(_.exists(_.isInstanceOf[GetStructField])),
      "expected GetStructField to appear in the optimized project list")
  }

  test("Paimon Variant: scan output type changes from VariantType to StructType after pushdown") {
    sql(s"""
           |CREATE TABLE T (id INT, v VARIANT)
           |TBLPROPERTIES ('parquet.variant.shreddingSchema' = '$shreddedSchema3')
           |""".stripMargin)
    sql("INSERT INTO T VALUES (1, parse_json('{\"age\":26,\"city\":\"Beijing\",\"score\":9.5}'))")

    // Without pushdown (direct reference): the variant column stays VariantType in the scan.
    val qFull = "SELECT v FROM T"
    val fullOutput = sql(qFull).queryExecution.optimizedPlan
      .collectFirst {
        case r: org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation => r
      }
      .get
      .output
    val variantColFull = fullOutput.find(_.name == "v").get
    assert(
      variantColFull.dataType == VariantType,
      "without pushdown, the scan output type for 'v' should remain VariantType")

    // With pushdown (only VariantGet accesses): the variant column becomes a StructType
    // whose fields correspond only to the accessed sub-columns (age + score, not city).
    val qPushed =
      "SELECT variant_get(v, '$.age', 'int'), variant_get(v, '$.score', 'double') FROM T"
    val pushedOutput = sql(qPushed).queryExecution.optimizedPlan
      .collectFirst {
        case r: org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation => r
      }
      .get
      .output
    val variantColPushed = pushedOutput.find(_.name == "v").get
    assert(
      variantColPushed.dataType.isInstanceOf[StructType],
      "after pushdown, the scan output type for 'v' should be StructType (shredded sub-schema)")
    assert(
      variantColPushed.dataType.asInstanceOf[StructType].length == 2,
      "the projected StructType should contain exactly 2 fields (age, score), not all 3")
  }
}
