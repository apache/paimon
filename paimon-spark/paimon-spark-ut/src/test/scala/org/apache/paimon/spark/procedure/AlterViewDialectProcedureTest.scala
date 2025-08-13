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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.spark.catalog.SupportView

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions

class AlterViewDialectProcedureTest extends PaimonSparkTestWithRestCatalogBase {

  test(s"test alter view dialect procedure") {
    val viewName = "view_test"
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING)
                 |""".stripMargin)

    val query = "SELECT * FROM T WHERE `id` > 1";
    spark.sql(s"""
                 |CREATE VIEW $viewName as $query
                 |""".stripMargin)
    val checkViewQuery = (view: String, query: String) =>
      Assertions
        .assertThat(
          spark
            .sql(s"desc extended $view")
            .filter("col_name = 'View Text'")
            .select("data_type")
            .collect()(0)(0))
        .isEqualTo(query)

    checkViewQuery(viewName, query)

    checkAnswer(
      spark.sql(s"CALL sys.alter_view_dialect('$viewName', 'drop', '${SupportView.DIALECT}')"),
      Row(true))

    checkAnswer(
      spark.sql(
        s"CALL sys.alter_view_dialect('$viewName', 'add', '${SupportView.DIALECT}', '$query')"),
      Row(true))

    checkViewQuery(viewName, query)

    val newQuery = "SELECT * FROM T WHERE `id` > 2";

    checkAnswer(
      spark.sql(
        s"CALL sys.alter_view_dialect('$viewName', 'update', '${SupportView.DIALECT}', '$newQuery')"),
      Row(true))

    checkViewQuery(viewName, newQuery)

    checkAnswer(
      spark.sql(s"CALL sys.alter_view_dialect(`view` => '$viewName', `action` => 'drop')"),
      Row(true))

    checkAnswer(
      spark.sql(
        s"CALL sys.alter_view_dialect(`view` => '$viewName', `action` => 'add', `query` => '$query')"),
      Row(true))
    checkViewQuery(viewName, query)

    checkAnswer(
      spark.sql(
        s"CALL sys.alter_view_dialect(`view` => '$viewName', `action` => 'update', `query` => '$newQuery')"),
      Row(true))
    checkViewQuery(viewName, newQuery)
  }
}
