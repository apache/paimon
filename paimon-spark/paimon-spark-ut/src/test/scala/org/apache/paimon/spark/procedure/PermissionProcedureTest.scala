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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.data.BinaryString
import org.apache.paimon.management.{PermissionResource, ResourceType}
import org.apache.paimon.predicate.{ConcatTransform, Equal, FieldRef, FieldTransform, LeafPredicate}
import org.apache.paimon.spark.{PaimonSparkTestBase, PaimonSparkTestWithRestCatalogBase}
import org.apache.paimon.types.DataTypes
import org.apache.paimon.utils.JsonSerdeUtil

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

import java.util.{Arrays, Collections}

/** End-to-end tests for permission and policy management procedures. */
class PermissionProcedureTest extends PaimonSparkTestWithRestCatalogBase {

  test("grant, list and idempotently revoke a permission") {
    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'table',
                  |  access => 'select',
                  |  principal => 'analyst',
                  |  database => 'sales',
                  |  table => 'orders',
                  |  expire_time => '2027-01-01T00:00:00Z')
                  |""".stripMargin),
      Row(true)
    )

    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'TABLE',
                  |  access => 'SELECT',
                  |  principal => 'analyst',
                  |  database => 'sales',
                  |  table => 'orders',
                  |  expire_time => '2028-01-01T00:00:00Z')
                  |""".stripMargin),
      Row(true)
    )

    val listed = spark.sql("""CALL sys.list_permissions(
                             |  resource_type => 'TABLE',
                             |  database => 'sales',
                             |  table => 'orders',
                             |  principal => 'analyst')
                             |""".stripMargin)
    assertThat(listed.columns).containsExactly(
      "resource_type",
      "database",
      "table",
      "function",
      "view",
      "access",
      "principal",
      "column_names",
      "excluded_column_names",
      "expire_time",
      "next_page_token")
    val assignments = listed.collect()
    assertThat(assignments).hasSize(1)
    val assignment = assignments.head
    assertThat(assignment.getString(0)).isEqualTo("TABLE")
    assertThat(assignment.getString(1)).isEqualTo("sales")
    assertThat(assignment.getString(2)).isEqualTo("orders")
    assertThat(assignment.getString(5)).isEqualTo("SELECT")
    assertThat(assignment.getString(6)).isEqualTo("analyst")
    assertThat(assignment.isNullAt(7)).isTrue
    assertThat(assignment.isNullAt(8)).isTrue
    assertThat(assignment.getString(9)).isEqualTo("2028-01-01T00:00:00Z")
    assertThat(assignment.isNullAt(10)).isTrue

    val revoke = """CALL sys.revoke_permission(
                   |  resource_type => 'TABLE',
                   |  access => 'SELECT',
                   |  principal => 'analyst',
                   |  database => 'sales',
                   |  table => 'orders')
                   |""".stripMargin
    checkAnswer(spark.sql(revoke), Row(true))
    checkAnswer(spark.sql(revoke), Row(true))
    checkAnswer(
      spark.sql("""CALL sys.list_permissions(
                  |  resource_type => 'TABLE',
                  |  database => 'sales',
                  |  table => 'orders',
                  |  principal => 'analyst')
                  |""".stripMargin),
      Nil
    )
  }

  test("list permissions supports opaque pagination tokens") {
    grantCatalogPermission("first")
    grantCatalogPermission("second")

    val first = spark
      .sql("CALL sys.list_permissions(resource_type => 'CATALOG', max_results => 1)")
      .head()
    assertThat(first.getString(6)).isEqualTo("first")
    assertThat(first.getString(10)).isEqualTo("1")

    val second = spark
      .sql(
        "CALL sys.list_permissions(resource_type => 'CATALOG', max_results => 1, page_token => '1')"
      )
      .head()
    assertThat(second.getString(6)).isEqualTo("second")
    assertThat(second.isNullAt(10)).isTrue
  }

  test("grant, replace, list and enforce column permissions") {
    restCatalogServer.setQueryPrincipals(Collections.singleton("analyst"))

    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'COLUMN',
                  |  access => 'SELECT',
                  |  principal => 'analyst',
                  |  database => 'sales',
                  |  table => 'orders',
                  |  column_names => array('id', 'region'))
                  |""".stripMargin),
      Row(true)
    )

    val included = spark.sql("""CALL sys.list_permissions(
                               |  resource_type => 'COLUMN',
                               |  database => 'sales',
                               |  table => 'orders',
                               |  principal => 'analyst')
                               |""".stripMargin)
    assertThat(included.columns).containsExactly(
      "resource_type",
      "database",
      "table",
      "function",
      "view",
      "access",
      "principal",
      "column_names",
      "excluded_column_names",
      "expire_time",
      "next_page_token")
    assertThat(included.head().getSeq[String](7)).isEqualTo(Seq("id", "region"))
    assertThat(included.head().isNullAt(8)).isTrue

    checkAnswer(spark.sql("SELECT id, region FROM paimon.sales.orders"), Nil)
    val deniedEmail = intercept[Exception] {
      spark.sql("SELECT email FROM paimon.sales.orders").collect()
    }
    assertThat(deniedEmail.getMessage).contains("permission")

    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'COLUMN',
                  |  access => 'SELECT',
                  |  principal => 'analyst',
                  |  database => 'sales',
                  |  table => 'orders',
                  |  excluded_column_names => array('region'))
                  |""".stripMargin),
      Row(true)
    )
    checkAnswer(spark.sql("SELECT id, email FROM paimon.sales.orders"), Nil)
    val deniedRegion = intercept[Exception] {
      spark.sql("SELECT region FROM paimon.sales.orders").collect()
    }
    assertThat(deniedRegion.getMessage).contains("permission")

    restCatalogServer.registerManagementPrincipal("limited")
    restCatalogServer.setQueryPrincipals(new java.util.HashSet(Arrays.asList("analyst", "limited")))
    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'COLUMN', access => 'SELECT',
                  |  principal => 'limited', database => 'sales', table => 'orders',
                  |  column_names => array('id', 'region'))
                  |""".stripMargin),
      Row(true)
    )
    checkAnswer(spark.sql("SELECT id FROM paimon.sales.orders"), Nil)
    val deniedByIntersection = intercept[Exception] {
      spark.sql("SELECT email FROM paimon.sales.orders").collect()
    }
    assertThat(deniedByIntersection.getMessage).contains("permission")

    checkAnswer(
      spark.sql("""CALL sys.revoke_permission(
                  |  resource_type => 'COLUMN', access => 'SELECT',
                  |  principal => 'analyst', database => 'sales', table => 'orders')
                  |""".stripMargin),
      Row(true)
    )
  }

  test("column permission validates query authorization and referenced columns") {
    spark.sql("CREATE TABLE paimon.sales.disabled_columns (id INT)")
    val disabled = intercept[Exception] {
      spark
        .sql("""CALL sys.grant_permission(
               |  resource_type => 'COLUMN', access => 'SELECT',
               |  principal => 'analyst', database => 'sales', table => 'disabled_columns',
               |  column_names => array('id'))
               |""".stripMargin)
        .collect()
    }
    assertThat(disabled.getMessage).contains("query-auth.enabled=true")

    val missing = intercept[Exception] {
      spark
        .sql("""CALL sys.grant_permission(
               |  resource_type => 'COLUMN', access => 'SELECT',
               |  principal => 'analyst', database => 'sales', table => 'orders',
               |  excluded_column_names => array('missing'))
               |""".stripMargin)
        .collect()
    }
    assertThat(missing.getMessage).contains("Permission column does not exist")
  }

  test("create, replace, apply and idempotently drop table data policies") {
    restCatalogServer.setQueryPrincipals(Collections.singleton("analyst"))
    spark.sql(
      "INSERT OVERWRITE paimon.sales.orders VALUES " +
        "(1, 'APAC', 'apac@example.com'), (2, 'EMEA', 'emea@example.com')")

    val apacFilter = stringEqualsPredicate(1, "region", "APAC")
    val emeaFilter = stringEqualsPredicate(1, "region", "EMEA")
    val emailMask = concatFieldTransform(1, "region", "-masked")

    checkAnswer(
      spark.sql(s"""CALL sys.create_policy(
                   |  database => 'sales',
                   |  table => 'orders',
                   |  policy_type => 'ROW_FILTER',
                   |  principal => 'analyst',
                   |  predicate_json => ${sqlLiteral(apacFilter)})
                   |""".stripMargin),
      Row(true)
    )

    val listed = spark.sql("""CALL sys.list_policies(
                             |  database => 'sales',
                             |  table => 'orders',
                             |  policy_type => 'ROW_FILTER',
                             |  principal => 'analyst')
                             |""".stripMargin)
    assertThat(listed.columns).containsExactly(
      "database",
      "table",
      "policy_type",
      "principal",
      "predicate_json",
      "on_column",
      "transform_json",
      "next_page_token")
    val direct = listed.head()
    assertThat(direct.getString(2)).isEqualTo("ROW_FILTER")
    assertThat(direct.getString(3)).isEqualTo("analyst")
    assertThat(direct.getString(4)).contains("\"name\":\"region\"")
    assertThat(direct.isNullAt(5)).isTrue
    assertThat(direct.isNullAt(6)).isTrue
    checkAnswer(
      spark.sql("SELECT id, region, email FROM paimon.sales.orders ORDER BY id"),
      Row(1, "APAC", "apac@example.com")
    )

    checkAnswer(
      spark.sql(s"""CALL sys.create_or_replace_policy(
                   |  database => 'sales',
                   |  table => 'orders',
                   |  policy_type => 'ROW_FILTER',
                   |  principal => 'analyst',
                   |  predicate_json => ${sqlLiteral(emeaFilter)})
                   |""".stripMargin),
      Row(true)
    )
    val replaced = spark
      .sql("""CALL sys.list_policies(
             |  database => 'sales',
             |  table => 'orders',
             |  policy_type => 'ROW_FILTER',
             |  principal => 'analyst')
             |""".stripMargin)
      .head()
    assertThat(replaced.getString(4)).contains("EMEA")

    checkAnswer(
      spark.sql(s"""CALL sys.create_policy(
                   |  database => 'sales',
                   |  table => 'orders',
                   |  policy_type => 'COLUMN_MASKING',
                   |  principal => 'analyst',
                   |  on_column => 'email',
                   |  transform_json => ${sqlLiteral(emailMask)})
                   |""".stripMargin),
      Row(true)
    )

    val mask = spark
      .sql("""CALL sys.list_policies(
             |  database => 'sales',
             |  table => 'orders',
             |  policy_type => 'COLUMN_MASKING')
             |""".stripMargin)
      .head()
    assertThat(mask.getString(2)).isEqualTo("COLUMN_MASKING")
    assertThat(mask.getString(3)).isEqualTo("analyst")
    assertThat(mask.isNullAt(4)).isTrue
    assertThat(mask.getString(5)).isEqualTo("email")
    assertThat(mask.getString(6)).contains("-masked")

    checkAnswer(
      spark.sql("SELECT id, region, email FROM paimon.sales.orders ORDER BY id"),
      Row(2, "EMEA", "EMEA-masked")
    )
    checkAnswer(
      spark.sql("SELECT email FROM paimon.sales.orders"),
      Row("EMEA-masked")
    )
    checkAnswer(
      spark.sql("""CALL sys.drop_policy(
                  |  database => 'sales',
                  |  table => 'orders',
                  |  policy_type => 'ROW_FILTER',
                  |  principal => 'analyst',
                  |  if_exists => true)
                  |""".stripMargin),
      Row(true)
    )
    checkAnswer(
      spark.sql("""CALL sys.drop_policy(
                  |  database => 'sales',
                  |  table => 'orders',
                  |  policy_type => 'ROW_FILTER',
                  |  principal => 'analyst',
                  |  if_exists => true)
                  |""".stripMargin),
      Row(true)
    )
    checkAnswer(
      spark.sql("""CALL sys.list_policies(
                  |  database => 'sales',
                  |  table => 'orders',
                  |  policy_type => 'ROW_FILTER',
                  |  principal => 'analyst')
                  |""".stripMargin),
      Nil
    )
    spark.sql(
      "CALL sys.drop_policy(database => 'sales', table => 'orders', " +
        "policy_type => 'COLUMN_MASKING', " +
        "principal => 'analyst', column => 'email', if_exists => true)")
  }

  test("policy creation validates query authorization, JSON and columns") {
    val validFilter = intEqualsPredicate(0, "id", 1)
    spark.sql("CREATE TABLE paimon.sales.disabled_orders (id INT)")

    val disabled = intercept[Exception] {
      spark
        .sql(s"""CALL sys.create_policy(
                |  database => 'sales', table => 'disabled_orders',
                |  policy_type => 'ROW_FILTER', predicate_json => ${sqlLiteral(validFilter)},
                |  principal => 'analyst')
                |""".stripMargin)
        .collect()
    }
    assertThat(disabled.getMessage).contains("query-auth.enabled=true")

    val malformed = intercept[Exception] {
      spark
        .sql("""CALL sys.create_policy(
               |  database => 'sales', table => 'orders',
               |  policy_type => 'ROW_FILTER', predicate_json => '{bad',
               |  principal => 'analyst')
               |""".stripMargin)
        .collect()
    }
    assertThat(malformed.getMessage).contains("Unexpected character")

    val unknownColumnFilter = stringEqualsPredicate(0, "unknown", "APAC")
    val missingColumn = intercept[Exception] {
      spark
        .sql(s"""CALL sys.create_policy(
                |  database => 'sales', table => 'orders',
                |  policy_type => 'ROW_FILTER', predicate_json => ${sqlLiteral(unknownColumnFilter)},
                |  principal => 'analyst')
                |""".stripMargin)
        .collect()
    }
    assertThat(missingColumn.getMessage).contains("column unknown")

    val missingPrincipal = intercept[Exception] {
      spark
        .sql(s"""CALL sys.create_policy(
                |  database => 'sales', table => 'orders',
                |  policy_type => 'ROW_FILTER', predicate_json => ${sqlLiteral(validFilter)},
                |  principal => 'missing')
                |""".stripMargin)
        .collect()
    }
    assertThat(missingPrincipal.getMessage).contains("principal does not exist")

    val jsonNull = intercept[Exception] {
      spark
        .sql("""CALL sys.create_policy(
               |  database => 'sales', table => 'orders',
               |  policy_type => 'ROW_FILTER', predicate_json => 'null',
               |  principal => 'analyst')
               |""".stripMargin)
        .collect()
    }
    assertThat(jsonNull.getMessage).contains("JSON null")

    val mixedDefinition = intercept[Exception] {
      spark
        .sql(s"""CALL sys.create_policy(
                |  database => 'sales', table => 'orders',
                |  policy_type => 'ROW_FILTER', predicate_json => ${sqlLiteral(validFilter)},
                |  transform_json => ${sqlLiteral(constantStringTransform("****"))},
                |  principal => 'analyst')
                |""".stripMargin)
        .collect()
    }
    assertThat(mixedDefinition.getMessage).contains("cannot specify transform")
  }

  test("list policies supports opaque pagination tokens") {
    val filter = intEqualsPredicate(0, "id", 1)
    Seq("first", "second").foreach {
      principal =>
        checkAnswer(
          spark.sql(s"""CALL sys.create_policy(
                       |  database => 'sales', table => 'orders',
                       |  policy_type => 'ROW_FILTER', principal => '$principal',
                       |  predicate_json => ${sqlLiteral(filter)})
                       |""".stripMargin),
          Row(true)
        )
    }
    try {
      val first = spark
        .sql("CALL sys.list_policies(database => 'sales', table => 'orders', max_results => 1)")
        .head()
      assertThat(first.getString(3)).isEqualTo("first")
      assertThat(first.getString(7)).isEqualTo("1")

      val second = spark
        .sql(
          "CALL sys.list_policies(database => 'sales', table => 'orders', " +
            "max_results => 1, page_token => '1')")
        .head()
      assertThat(second.getString(3)).isEqualTo("second")
      assertThat(second.isNullAt(7)).isTrue
    } finally {
      Seq("first", "second").foreach {
        principal =>
          spark.sql(
            "CALL sys.drop_policy(database => 'sales', table => 'orders', " +
              s"policy_type => 'ROW_FILTER', principal => '$principal', if_exists => true)")
      }
    }
  }

  test("table lifecycle preserves policy enforcement and rejects unsafe schema changes") {
    restCatalogServer.setQueryPrincipals(Collections.singleton("analyst"))
    spark.sql("""CREATE TABLE paimon.sales.lifecycle_orders (
                |  id INT,
                |  region STRING)
                |TBLPROPERTIES ('query-auth.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO paimon.sales.lifecycle_orders VALUES (1, 'APAC'), (2, 'EMEA')")
    val lifecycleFilter = stringEqualsPredicate(1, "region", "APAC")
    checkAnswer(
      spark.sql(s"""CALL sys.create_policy(
                   |  database => 'sales', table => 'lifecycle_orders',
                   |  policy_type => 'ROW_FILTER', predicate_json => ${sqlLiteral(lifecycleFilter)},
                   |  principal => 'analyst')
                   |""".stripMargin),
      Row(true)
    )

    spark.sql("ALTER TABLE paimon.sales.lifecycle_orders RENAME TO paimon.sales.renamed_orders")
    assertThat(
      spark
        .sql("CALL sys.list_policies(database => 'sales', table => 'renamed_orders', " +
          "policy_type => 'ROW_FILTER', principal => 'analyst')")
        .head()
        .getString(1)).isEqualTo("renamed_orders")
    assertThat(
      paimonCatalog
        .authTableQuery(Identifier.create("sales", "renamed_orders"), null)
        .extractPredicate()).isNotNull

    val disableAuth = intercept[Exception] {
      spark
        .sql("ALTER TABLE paimon.sales.renamed_orders " +
          "SET TBLPROPERTIES ('query-auth.enabled' = 'false')")
        .collect()
    }
    assertThat(disableAuth.getMessage).contains("Cannot disable query-auth.enabled")

    val renameColumn = intercept[Exception] {
      spark
        .sql("ALTER TABLE paimon.sales.renamed_orders RENAME COLUMN region TO area")
        .collect()
    }
    assertThat(renameColumn.getMessage).contains("column region")

    spark.sql("DROP TABLE paimon.sales.renamed_orders")
    spark.sql("""CREATE TABLE paimon.sales.renamed_orders (
                |  id INT,
                |  region STRING)
                |TBLPROPERTIES ('query-auth.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO paimon.sales.renamed_orders VALUES (1, 'APAC'), (2, 'EMEA')")
    checkAnswer(
      spark.sql("SELECT id, region FROM paimon.sales.renamed_orders ORDER BY id"),
      Seq(Row(1, "APAC"), Row(2, "EMEA"))
    )
    checkAnswer(
      spark.sql(
        "CALL sys.list_policies(database => 'sales', table => 'renamed_orders', " +
          "policy_type => 'ROW_FILTER', principal => 'analyst')"),
      Nil
    )
    assertThat(
      paimonCatalog
        .authTableQuery(Identifier.create("sales", "renamed_orders"), null)
        .extractPredicate()).isNull
  }

  test("management endpoints enforce target authorization") {
    val resource =
      new PermissionResource(ResourceType.TABLE, "sales", "orders", null, null)
    restCatalogServer.denyManagementPermission(resource)
    try {
      val permissionError = intercept[Exception] {
        spark
          .sql("""CALL sys.grant_permission(
                 |  resource_type => 'TABLE', access => 'SELECT',
                 |  principal => 'analyst',
                 |  database => 'sales', table => 'orders')
                 |""".stripMargin)
          .collect()
      }
      assertThat(permissionError.getMessage).contains("cannot manage permissions")

      val policyError = intercept[Exception] {
        spark
          .sql("CALL sys.list_policies(database => 'sales', table => 'orders')")
          .collect()
      }
      assertThat(policyError.getMessage).contains("cannot manage permissions")
    } finally {
      restCatalogServer.allowManagementPermission(resource)
    }
  }

  private def grantCatalogPermission(principal: String): Unit = {
    checkAnswer(
      spark.sql(s"""CALL sys.grant_permission(
                   |  resource_type => 'CATALOG',
                   |  access => 'CREATEDATABASE',
                   |  principal => '$principal')
                   |""".stripMargin),
      Row(true)
    )
  }

  private def stringEqualsPredicate(index: Int, column: String, constant: String): String = {
    JsonSerdeUtil.toFlatJson(
      LeafPredicate.of(
        new FieldTransform(new FieldRef(index, column, DataTypes.STRING())),
        Equal.INSTANCE,
        Collections.singletonList(BinaryString.fromString(constant))))
  }

  private def intEqualsPredicate(index: Int, column: String, constant: Int): String = {
    JsonSerdeUtil.toFlatJson(
      LeafPredicate.of(
        new FieldTransform(new FieldRef(index, column, DataTypes.INT())),
        Equal.INSTANCE,
        Collections.singletonList(Integer.valueOf(constant))))
  }

  private def constantStringTransform(constant: String): String = {
    JsonSerdeUtil.toFlatJson(
      new ConcatTransform(Collections.singletonList(BinaryString.fromString(constant))))
  }

  private def concatFieldTransform(index: Int, column: String, suffix: String): String = {
    JsonSerdeUtil.toFlatJson(
      new ConcatTransform(Arrays
        .asList(new FieldRef(index, column, DataTypes.STRING()), BinaryString.fromString(suffix))))
  }

  private def sqlLiteral(value: String): String = {
    "'" + value.replace("'", "''") + "'"
  }
}

/** Management procedures must fail clearly for catalogs without the REST capability. */
class PermissionProcedureUnsupportedCatalogTest extends PaimonSparkTestBase {

  test("filesystem catalog does not expose permission management") {
    val error = intercept[IllegalArgumentException] {
      spark
        .sql("""CALL sys.grant_permission(
               |  resource_type => 'CATALOG',
               |  access => 'CREATEDATABASE',
               |  principal => 'admin')
               |""".stripMargin)
        .collect()
    }
    assertThat(error.getMessage).contains("does not support permission or policy management")
  }
}
