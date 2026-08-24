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

import org.apache.paimon.spark.{PaimonSparkTestBase, PaimonSparkTestWithRestCatalogBase}

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

/** End-to-end tests for permission management procedures. */
class PermissionProcedureTest extends PaimonSparkTestWithRestCatalogBase {

  test("grant, list and revoke a permission") {
    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'column',
                  |  access => 'select',
                  |  principal => 'role:analyst',
                  |  database => 'sales',
                  |  table => 'orders',
                  |  column_names => array('id', 'amount'),
                  |  expire_time => '2027-01-01T00:00:00Z')
                  |""".stripMargin),
      Row(true)
    )

    val permission = spark
      .sql("""CALL sys.list_permissions(
             |  principal => 'role:analyst',
             |  resource_type => 'TABLE')
             |""".stripMargin)
      .head()
    assertThat(permission.getString(0)).isEqualTo("COLUMN")
    assertThat(permission.isNullAt(1)).isTrue
    assertThat(permission.getString(2)).isEqualTo("sales")
    assertThat(permission.getString(3)).isEqualTo("orders")
    assertThat(permission.getString(6)).contains("\"columnNames\":[\"id\",\"amount\"]")
    assertThat(permission.getString(9)).isEqualTo("SELECT")
    assertThat(permission.getString(10)).isEqualTo("role:analyst")
    assertThat(permission.getString(11)).isEqualTo("2027-01-01T00:00:00Z")
    assertThat(permission.isNullAt(12)).isTrue

    checkAnswer(
      spark.sql("""CALL sys.revoke_permission(
                  |  resource_type => 'COLUMN',
                  |  access => 'SELECT',
                  |  principal => 'role:analyst',
                  |  database => 'sales',
                  |  table => 'orders')
                  |""".stripMargin),
      Row(true)
    )
    checkAnswer(
      spark.sql(
        "CALL sys.list_permissions(resource_type => 'COLUMN', principal => 'role:analyst')"
      ),
      Nil
    )
  }

  test("list permissions supports opaque pagination tokens") {
    grantCatalogPermission("role:first")
    grantCatalogPermission("role:second")

    val first = spark
      .sql("CALL sys.list_permissions(resource_type => 'CATALOG', max_results => 1)")
      .head()
    assertThat(first.getString(10)).isEqualTo("role:first")
    assertThat(first.getString(12)).isEqualTo("1")

    val second = spark
      .sql(
        "CALL sys.list_permissions(resource_type => 'CATALOG', max_results => 1, page_token => '1')"
      )
      .head()
    assertThat(second.getString(10)).isEqualTo("role:second")
    assertThat(second.isNullAt(12)).isTrue
  }

  test("policy payload is limited to its matching resource type") {
    val error = intercept[IllegalArgumentException] {
      spark
        .sql("""CALL sys.grant_permission(
               |  resource_type => 'TABLE',
               |  access => 'SELECT',
               |  principal => 'role:writer',
               |  database => 'sales',
               |  table => 'orders',
               |  column_names => array('id'))
               |""".stripMargin)
        .collect()
    }
    assertThat(error.getMessage)
      .contains("Columns are supported only for COLUMN permissions")
  }

  private def grantCatalogPermission(principal: String): Unit = {
    checkAnswer(
      spark.sql(s"""CALL sys.grant_permission(
                   |  resource_type => 'CATALOG_ALL',
                   |  access => 'SELECT',
                   |  principal => '$principal')
                   |""".stripMargin),
      Row(true)
    )
  }
}

/** Permission procedures must fail clearly for catalogs without the management capability. */
class PermissionProcedureUnsupportedCatalogTest extends PaimonSparkTestBase {

  test("filesystem catalog does not expose permission management") {
    val error = intercept[IllegalArgumentException] {
      spark
        .sql("""CALL sys.grant_permission(
               |  resource_type => 'CATALOG',
               |  access => 'ALL',
               |  principal => 'role:admin')
               |""".stripMargin)
        .collect()
    }
    assertThat(error.getMessage).contains("does not support permission management")
  }
}
