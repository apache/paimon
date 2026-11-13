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

package org.apache.paimon.spark.ossie

import org.apache.paimon.view.SemanticViewDefinition

import org.scalatest.funsuite.AnyFunSuite

class OssieModelTest extends AnyFunSuite {
  private val model =
    """version: 0.3.0.dev0
      |name: revenue
      |datasets:
      |  - name: orders
      |    source: orders
      |    fields:
      |      - name: amount
      |        expression: {sql: price}
      |    metrics:
      |      - name: revenue
      |        expression: {sql: SUM(amount)}
      |""".stripMargin

  private def parse(text: String): OssieModel =
    OssieModel.parse(new SemanticViewDefinition("ossie-yaml", text))

  test("parse the flat document and ignore preserved nonportable metadata") {
    val text = model +
      "nonportable_extensions:\n  - vendor_name: example\n    data: {nested: [1, true, null]}\n"
    val result = parse(text)
    assert(result.dataset == "orders")
    assert(result.fields == Seq(OssieField("amount", "price")))
    assert(result.metrics == Seq(OssieMetric("revenue", "SUM(amount)", modelScoped = false)))
    val portable = parse(
      model.replace(
        "{sql: SUM(amount)}",
        "{dialects: [{dialect: OSSIE_SQL_2026, expression: SUM(amount)}]}"))
    assert(portable == result)
  }

  test("validate the pinned schema and reject old document shapes and unknown core fields") {
    Seq(
      model.replace("version: 0.3.0.dev0", "version: 0.2.0.dev0"),
      model + "semantic_model: []\n",
      model + "custom_extensions: []\n",
      model.replace("expression: {sql: price}", "expression: {sql: 42}"),
      model.replace("source: orders", "source: orders\n    primary_key: [missing]"),
      model.replace("name: revenue\n        expression", "name: AMOUNT\n        expression")
    ).foreach(text => intercept[IllegalArgumentException](parse(text)))
  }

  test("reject multiple datasets and relationships instead of flattening joins") {
    // Add the second dataset before the first dataset's fields, retaining a valid document.
    val second =
      model.replace("datasets:\n", "datasets:\n  - name: customers\n    source: customers\n")
    assert(intercept[IllegalArgumentException](parse(second)).getMessage.contains("exactly one"))
    val relationship = model + "relationships:\n  - name: self\n    from: orders\n" +
      "    to: orders\n    from_columns: [amount]\n    to_columns: [amount]\n"
    assert(
      intercept[IllegalArgumentException](parse(relationship)).getMessage.contains("relationships"))
  }

  test("reject duplicate YAML keys, trailing documents, and nonfinite extension values") {
    Seq(
      model + "name: duplicate\n",
      model + "---\n" + model,
      model + "nonportable_extensions:\n  - vendor_name: test\n    data: {value: .inf}\n"
    ).foreach(text => intercept[Exception](parse(text)))
  }

  test("reject dialect-only expressions without a portable form") {
    val text = model.replace(
      "{sql: SUM(amount)}",
      "{dialects: [{dialect: UNKNOWN, expression: SUM(amount)}]}")
    assert(intercept[IllegalArgumentException](parse(text)).getMessage.contains("OSSIE_SQL_2026"))
    assert(
      intercept[IllegalArgumentException](
        OssieModel.parse(new SemanticViewDefinition("databricks-yaml", model))).getMessage
        .contains("ossie-yaml"))
  }

  test("extension data must be JSON values with string keys") {
    Seq(
      "2026-11-13",
      "2026-11-13T01:02:03Z",
      ".nan",
      "!!binary SGVsbG8=",
      "!!set {a: null}",
      "&cycle [*cycle]")
      .foreach {
        value =>
          val text =
            model + s"nonportable_extensions:\n  - vendor_name: test\n    data: {value: $value}\n"
          intercept[Exception](parse(text))
      }
    intercept[Exception](
      parse(model +
        "nonportable_extensions:\n  - vendor_name: test\n    data: {123: value}\n"))
    assert(parse(model +
      "nonportable_extensions:\n  - vendor_name: test\n    data: {date: '2026-11-13', nested: [null, true, 1]}\n").dataset == "orders")
  }
}
