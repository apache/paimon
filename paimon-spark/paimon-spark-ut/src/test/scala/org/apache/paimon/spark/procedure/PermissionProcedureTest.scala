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
import org.apache.paimon.management.{ColumnMask, PermissionResource, PrincipalRef, PrincipalType, ResourceType, RowFilter}
import org.apache.paimon.predicate.{ConcatTransform, Equal, FieldRef, FieldTransform, LeafPredicate, Predicate, Transform}
import org.apache.paimon.schema.TableSchema
import org.apache.paimon.spark.{PaimonSparkTestBase, PaimonSparkTestWithRestCatalogBase}
import org.apache.paimon.types.DataTypes

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

import java.util.Collections
import java.util.function.BiFunction

/** End-to-end tests for permission and policy management procedures. */
class PermissionProcedureTest extends PaimonSparkTestWithRestCatalogBase {

  test("grant, list and idempotently revoke a permission") {
    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'table',
                  |  access => 'select',
                  |  principal_type => 'role',
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
                  |  principal_type => 'ROLE',
                  |  principal => 'analyst',
                  |  database => 'sales',
                  |  table => 'orders',
                  |  expire_time => '2028-01-01T00:00:00Z')
                  |""".stripMargin),
      Row(true)
    )

    val assignments = spark
      .sql("""CALL sys.list_permissions(
             |  resource_type => 'TABLE',
             |  database => 'sales',
             |  table => 'orders',
             |  principal_type => 'ROLE',
             |  principal => 'analyst')
             |""".stripMargin)
      .collect()
    assertThat(assignments).hasSize(1)
    val assignment = assignments.head
    assertThat(assignment.getString(0)).isEqualTo("TABLE")
    assertThat(assignment.getString(1)).isEqualTo("SELF")
    assertThat(assignment.getString(2)).isEqualTo("sales")
    assertThat(assignment.getString(3)).isEqualTo("orders")
    assertThat(assignment.getString(6)).isEqualTo("SELECT")
    assertThat(assignment.getString(7)).isEqualTo("ROLE")
    assertThat(assignment.getString(8)).isEqualTo("analyst")
    assertThat(assignment.getString(9)).isEqualTo("2028-01-01T00:00:00Z")
    assertThat(assignment.isNullAt(10)).isTrue
    assertThat(assignment.isNullAt(11)).isTrue

    val revoke = """CALL sys.revoke_permission(
                   |  resource_type => 'TABLE',
                   |  access => 'SELECT',
                   |  principal_type => 'ROLE',
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
                  |  principal_type => 'ROLE',
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
    assertThat(first.getString(8)).isEqualTo("first")
    assertThat(first.getString(11)).isEqualTo("1")

    val second = spark
      .sql(
        "CALL sys.list_permissions(resource_type => 'CATALOG', max_results => 1, page_token => '1')"
      )
      .head()
    assertThat(second.getString(8)).isEqualTo("second")
    assertThat(second.isNullAt(11)).isTrue
  }

  test("create, replace, apply and idempotently drop table data policies") {
    restCatalogServer.registerRowFilterPolicyCompiler("security.filter_region", rowFilterCompiler)
    restCatalogServer.registerRowFilterPolicyCompiler(
      "security.filter_region_v2",
      rowFilterCompiler)
    restCatalogServer.registerColumnMaskPolicyCompiler("security.mask_email", columnMaskCompiler)
    restCatalogServer.setQueryPrincipals(
      Collections.singleton(new PrincipalRef(PrincipalType.ROLE, "analyst")))
    spark.sql(
      "INSERT OVERWRITE paimon.sales.orders VALUES " +
        "(1, 'APAC', 'apac@example.com'), (2, 'EMEA', 'emea@example.com')")

    checkAnswer(
      spark.sql("""CALL sys.create_policy(
                  |  database => 'sales',
                  |  table => 'orders',
                  |  name => 'region_filter',
                  |  policy_type => 'ROW_FILTER',
                  |  function_name => 'security.filter_region',
                  |  to_principals => array('ROLE:analyst'),
                  |  function_arguments => array('column:region', 'constant:APAC'),
                  |  comment => 'Filter rows by region')
                  |""".stripMargin),
      Row(true)
    )

    val direct = spark
      .sql("""CALL sys.list_policies(
             |  database => 'sales',
             |  table => 'orders',
             |  name => 'region_filter')
             |""".stripMargin)
      .head()
    assertThat(direct.getString(2)).isEqualTo("region_filter")
    assertThat(direct.getString(3)).isEqualTo("ROW_FILTER")
    assertThat(direct.getString(4)).isEqualTo("security.filter_region")
    assertThat(direct.isNullAt(5)).isTrue
    assertThat(direct.getString(6)).contains("\"column\":\"region\"")
    assertThat(direct.getString(7)).contains("\"type\":\"ROLE\"")
    checkAnswer(
      spark.sql("SELECT id, region, email FROM paimon.sales.orders ORDER BY id"),
      Row(1, "APAC", "apac@example.com")
    )

    checkAnswer(
      spark.sql("""CALL sys.create_or_replace_policy(
                  |  database => 'sales',
                  |  table => 'orders',
                  |  name => 'region_filter',
                  |  policy_type => 'ROW_FILTER',
                  |  function_name => 'security.filter_region_v2',
                  |  function_arguments => array('column:region', 'constant:EMEA'),
                  |  to_principals => array('ROLE:analyst'),
                  |  except_principals => array('USER:admin'))
                  |""".stripMargin),
      Row(true)
    )
    val replaced = spark
      .sql("""CALL sys.list_policies(
             |  database => 'sales',
             |  table => 'orders',
             |  name => 'region_filter')
             |""".stripMargin)
      .head()
    assertThat(replaced.getString(4)).isEqualTo("security.filter_region_v2")

    checkAnswer(
      spark.sql("""CALL sys.create_policy(
                  |  database => 'sales',
                  |  table => 'orders',
                  |  name => 'mask_email',
                  |  policy_type => 'COLUMN_MASKING',
                  |  function_name => 'security.mask_email',
                  |  on_column => 'email',
                  |  function_arguments => array('constant:****'),
                  |  to_principals => array('ROLE:analyst'),
                  |  except_principals => array('USER:admin'))
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
    assertThat(mask.getString(2)).isEqualTo("mask_email")
    assertThat(mask.getString(5)).isEqualTo("email")
    assertThat(mask.getString(6)).contains("\"constant\":\"****\"")
    assertThat(mask.getString(8)).contains("\"id\":\"admin\"")

    checkAnswer(
      spark.sql("SELECT id, region, email FROM paimon.sales.orders ORDER BY id"),
      Row(2, "EMEA", "****")
    )
    val exemptPrincipals = new java.util.HashSet[PrincipalRef]()
    exemptPrincipals.add(new PrincipalRef(PrincipalType.ROLE, "analyst"))
    exemptPrincipals.add(new PrincipalRef(PrincipalType.USER, "admin"))
    restCatalogServer.setQueryPrincipals(exemptPrincipals)
    checkAnswer(
      spark.sql("SELECT id, region, email FROM paimon.sales.orders ORDER BY id"),
      Seq(
        Row(1, "APAC", "apac@example.com"),
        Row(2, "EMEA", "emea@example.com")
      )
    )

    checkAnswer(
      spark.sql("""CALL sys.drop_policy(
                  |  database => 'sales',
                  |  table => 'orders',
                  |  name => 'region_filter',
                  |  if_exists => true)
                  |""".stripMargin),
      Row(true)
    )
    checkAnswer(
      spark.sql("""CALL sys.drop_policy(
                  |  database => 'sales',
                  |  table => 'orders',
                  |  name => 'region_filter',
                  |  if_exists => true)
                  |""".stripMargin),
      Row(true)
    )
    checkAnswer(
      spark.sql("""CALL sys.list_policies(
                  |  database => 'sales',
                  |  table => 'orders',
                  |  name => 'region_filter')
                  |""".stripMargin),
      Nil
    )
    spark.sql(
      "CALL sys.drop_policy(database => 'sales', table => 'orders', " +
        "name => 'mask_email', if_exists => true)")
  }

  test("policy creation validates query authorization, functions and columns") {
    restCatalogServer.registerRowFilterPolicyFunction(
      "security.valid_filter",
      LeafPredicate.of(
        new FieldTransform(new FieldRef(0, "id", DataTypes.INT())),
        Equal.INSTANCE,
        Collections.singletonList(Integer.valueOf(1)))
    )
    spark.sql("CREATE TABLE paimon.sales.disabled_orders (id INT)")

    val disabled = intercept[Exception] {
      spark
        .sql("""CALL sys.create_policy(
               |  database => 'sales', table => 'disabled_orders', name => 'disabled',
               |  policy_type => 'ROW_FILTER', function_name => 'security.valid_filter',
               |  to_principals => array('ROLE:analyst'))
               |""".stripMargin)
        .collect()
    }
    assertThat(disabled.getMessage).contains("query-auth.enabled=true")

    val missingFunction = intercept[Exception] {
      spark
        .sql("""CALL sys.create_policy(
               |  database => 'sales', table => 'orders', name => 'missing_function',
               |  policy_type => 'ROW_FILTER', function_name => 'security.missing',
               |  to_principals => array('ROLE:analyst'))
               |""".stripMargin)
        .collect()
    }
    assertThat(missingFunction.getMessage).contains("does not exist")

    val missingColumn = intercept[Exception] {
      spark
        .sql("""CALL sys.create_policy(
               |  database => 'sales', table => 'orders', name => 'missing_column',
               |  policy_type => 'ROW_FILTER', function_name => 'security.valid_filter',
               |  function_arguments => array('column:unknown'),
               |  to_principals => array('ROLE:analyst'))
               |""".stripMargin)
        .collect()
    }
    assertThat(missingColumn.getMessage).contains("does not exist")

    val missingPrincipal = intercept[Exception] {
      spark
        .sql("""CALL sys.create_policy(
               |  database => 'sales', table => 'orders', name => 'missing_principal',
               |  policy_type => 'ROW_FILTER', function_name => 'security.valid_filter',
               |  to_principals => array('ROLE:missing'))
               |""".stripMargin)
        .collect()
    }
    assertThat(missingPrincipal.getMessage).contains("principal does not exist")
  }

  test("table lifecycle preserves policy enforcement and rejects unsafe schema changes") {
    restCatalogServer.registerRowFilterPolicyCompiler(
      "security.lifecycle_filter",
      rowFilterCompiler)
    restCatalogServer.setQueryPrincipals(
      Collections.singleton(new PrincipalRef(PrincipalType.ROLE, "analyst")))
    spark.sql("""CREATE TABLE paimon.sales.lifecycle_orders (
                |  id INT,
                |  region STRING)
                |TBLPROPERTIES ('query-auth.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO paimon.sales.lifecycle_orders VALUES (1, 'APAC'), (2, 'EMEA')")
    checkAnswer(
      spark.sql("""CALL sys.create_policy(
                  |  database => 'sales', table => 'lifecycle_orders', name => 'region_filter',
                  |  policy_type => 'ROW_FILTER', function_name => 'security.lifecycle_filter',
                  |  function_arguments => array('column:region', 'constant:APAC'),
                  |  to_principals => array('ROLE:analyst'))
                  |""".stripMargin),
      Row(true)
    )

    spark.sql("ALTER TABLE paimon.sales.lifecycle_orders RENAME TO paimon.sales.renamed_orders")
    assertThat(
      spark
        .sql("CALL sys.list_policies(database => 'sales', table => 'renamed_orders', " +
          "name => 'region_filter')")
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
    assertThat(renameColumn.getMessage).contains("policy argument column region")

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
          "name => 'region_filter')"),
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
                 |  principal_type => 'ROLE', principal => 'analyst',
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

  test("include inherited permissions resolves only resource ancestry") {
    grantCatalogPermission("reader")

    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'CATALOG',
                  |  scope => 'DESCENDANTS',
                  |  access => 'EXECUTE',
                  |  principal_type => 'ROLE',
                  |  principal => 'function_reader')
                  |""".stripMargin),
      Row(true)
    )

    checkAnswer(
      spark.sql("""CALL sys.list_permissions(
                  |  resource_type => 'TABLE',
                  |  database => 'sales',
                  |  table => 'orders',
                  |  principal_type => 'ROLE',
                  |  principal => 'reader')
                  |""".stripMargin),
      Nil
    )

    val inherited = spark
      .sql("""CALL sys.list_permissions(
             |  resource_type => 'TABLE',
             |  database => 'sales',
             |  table => 'orders',
             |  scope => 'SELF',
             |  principal_type => 'ROLE',
             |  principal => 'reader',
             |  include_inherited => true)
             |""".stripMargin)
      .head()
    assertThat(inherited.getString(0)).isEqualTo("TABLE")
    assertThat(inherited.getString(1)).isEqualTo("SELF")
    assertThat(inherited.getString(10)).contains("\"type\":\"CATALOG\"")

    checkAnswer(
      spark.sql("""CALL sys.list_permissions(
                  |  resource_type => 'TABLE',
                  |  database => 'sales',
                  |  table => 'orders',
                  |  principal_type => 'ROLE',
                  |  principal => 'function_reader',
                  |  include_inherited => true)
                  |""".stripMargin),
      Nil
    )
  }

  private def grantCatalogPermission(principal: String): Unit = {
    checkAnswer(
      spark.sql(s"""CALL sys.grant_permission(
                   |  resource_type => 'CATALOG',
                   |  scope => 'DESCENDANTS',
                   |  access => 'SELECT',
                   |  principal_type => 'ROLE',
                   |  principal => '$principal')
                   |""".stripMargin),
      Row(true)
    )
  }

  private val rowFilterCompiler = new BiFunction[TableSchema, RowFilter, Predicate] {
    override def apply(schema: TableSchema, filter: RowFilter): Predicate = {
      val arguments = filter.getFunctionArguments
      require(arguments.size() == 2)
      val column = arguments.get(0).getColumn
      val constant = arguments.get(1).getConstant
      require(column != null && constant != null)
      val index = schema.fieldNames().indexOf(column)
      require(index >= 0)
      LeafPredicate.of(
        new FieldTransform(new FieldRef(index, column, schema.fields().get(index).`type`())),
        Equal.INSTANCE,
        Collections.singletonList(BinaryString.fromString(constant))
      )
    }
  }

  private val columnMaskCompiler = new BiFunction[TableSchema, ColumnMask, Transform] {
    override def apply(schema: TableSchema, mask: ColumnMask): Transform = {
      val arguments = mask.getFunctionArguments
      require(schema.fieldNames().contains(mask.getOnColumn))
      require(arguments.size() == 1 && arguments.get(0).getConstant != null)
      new ConcatTransform(
        Collections.singletonList(BinaryString.fromString(arguments.get(0).getConstant)))
    }
  }
}

/** Management procedures must fail clearly for catalogs without the REST capability. */
class PermissionProcedureUnsupportedCatalogTest extends PaimonSparkTestBase {

  test("filesystem catalog does not expose permission management") {
    val error = intercept[IllegalArgumentException] {
      spark
        .sql("""CALL sys.grant_permission(
               |  resource_type => 'CATALOG',
               |  access => 'USE_CATALOG',
               |  principal_type => 'ROLE',
               |  principal => 'admin')
               |""".stripMargin)
        .collect()
    }
    assertThat(error.getMessage).contains("does not support permission or policy management")
  }
}
