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
import org.apache.paimon.data.{BinaryString, GenericRow, Timestamp}
import org.apache.paimon.management.{PermissionResource, ResourceType}
import org.apache.paimon.predicate.{ConcatTransform, Equal, FieldRef, FieldTransform, LeafPredicate, LowerTransform, NotEqual}
import org.apache.paimon.schema.Schema
import org.apache.paimon.spark.{PaimonSparkTestBase, PaimonSparkTestWithRestCatalogBase, SparkTypeUtils}
import org.apache.paimon.spark.util.shim.TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType
import org.apache.paimon.types.DataTypes
import org.apache.paimon.utils.JsonSerdeUtil

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

import java.time.LocalDateTime
import java.util.{Arrays, Collections, Locale}

/** End-to-end tests for permission and policy management procedures. */
class PermissionProcedureTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.paimon.v1Function.enabled", "false")

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
    val pageToken = first.getString(10)
    assertThat(pageToken).isNotEmpty.isNotEqualTo("1")

    spark.sql(
      "CALL sys.revoke_permission(resource_type => 'CATALOG', " +
        "access => 'CREATEDATABASE', principal => 'first')")

    val second = spark
      .sql(
        "CALL sys.list_permissions(resource_type => 'CATALOG', max_results => 1, " +
          s"page_token => ${sqlLiteral(pageToken)})"
      )
      .head()
    assertThat(second.getString(6)).isEqualTo("second")
    assertThat(second.isNullAt(10)).isTrue
  }

  test("grant and list explicit descendant scopes") {
    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'CATALOG_ALL',
                  |  access => 'SELECT',
                  |  principal => 'analyst')
                  |""".stripMargin),
      Row(true)
    )
    val catalogAll = spark
      .sql(
        "CALL sys.list_permissions(resource_type => 'CATALOG_ALL', principal => 'analyst')"
      )
      .head()
    assertThat(catalogAll.getString(0)).isEqualTo("CATALOG_ALL")
    assertThat(catalogAll.isNullAt(1)).isTrue
    assertThat(catalogAll.getString(5)).isEqualTo("SELECT")

    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'DATABASE_ALL',
                  |  database => 'sales',
                  |  access => 'UPDATE',
                  |  principal => 'analyst')
                  |""".stripMargin),
      Row(true)
    )
    val databaseAll = spark
      .sql("""CALL sys.list_permissions(
             |  resource_type => 'DATABASE_ALL',
             |  database => 'sales',
             |  principal => 'analyst')
             |""".stripMargin)
      .head()
    assertThat(databaseAll.getString(0)).isEqualTo("DATABASE_ALL")
    assertThat(databaseAll.getString(1)).isEqualTo("sales")
    assertThat(databaseAll.isNullAt(2)).isTrue
    assertThat(databaseAll.getString(5)).isEqualTo("UPDATE")
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

  test("column permissions follow table and schema lifecycle") {
    restCatalogServer.setQueryPrincipals(Collections.singleton("analyst"))
    spark.sql("""CREATE TABLE paimon.sales.column_lifecycle (
                |  id INT,
                |  secret STRING)
                |TBLPROPERTIES ('query-auth.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO paimon.sales.column_lifecycle VALUES (1, 's1')")
    checkAnswer(
      spark.sql("""CALL sys.grant_permission(
                  |  resource_type => 'COLUMN', access => 'SELECT', principal => 'analyst',
                  |  database => 'sales', table => 'column_lifecycle',
                  |  excluded_column_names => array('secret'))
                  |""".stripMargin),
      Row(true)
    )

    spark.sql(
      "ALTER TABLE paimon.sales.column_lifecycle RENAME TO paimon.sales.renamed_column_lifecycle")
    var assignment = spark
      .sql(
        "CALL sys.list_permissions(resource_type => 'COLUMN', database => 'sales', " +
          "table => 'renamed_column_lifecycle', principal => 'analyst')")
      .head()
    assertThat(assignment.getString(2)).isEqualTo("renamed_column_lifecycle")
    assertThat(assignment.getSeq[String](8)).isEqualTo(Seq("secret"))

    val disableAuth = intercept[Exception] {
      spark
        .sql("ALTER TABLE paimon.sales.renamed_column_lifecycle " +
          "SET TBLPROPERTIES ('query-auth.enabled' = 'false')")
        .collect()
    }
    assertThat(disableAuth.getMessage).contains("Cannot disable query-auth.enabled")

    spark.sql(
      "ALTER TABLE paimon.sales.renamed_column_lifecycle " +
        "RENAME COLUMN secret TO private_secret")
    assignment = spark
      .sql(
        "CALL sys.list_permissions(resource_type => 'COLUMN', database => 'sales', " +
          "table => 'renamed_column_lifecycle', principal => 'analyst')")
      .head()
    assertThat(assignment.getSeq[String](8)).isEqualTo(Seq("private_secret"))
    val denied = intercept[Exception] {
      spark.sql("SELECT private_secret FROM paimon.sales.renamed_column_lifecycle").collect()
    }
    assertThat(denied.getMessage).contains("permission")

    spark.sql("""CALL sys.grant_permission(
                |  resource_type => 'COLUMN', access => 'SELECT', principal => 'analyst',
                |  database => 'sales', table => 'renamed_column_lifecycle',
                |  column_names => array('private_secret'))
                |""".stripMargin)
    val dropOnlyAllowedColumn = intercept[Exception] {
      spark
        .sql("ALTER TABLE paimon.sales.renamed_column_lifecycle DROP COLUMN private_secret")
        .collect()
    }
    assertThat(dropOnlyAllowedColumn.getMessage).contains("Cannot drop every allowed column")

    spark.sql("""CALL sys.grant_permission(
                |  resource_type => 'COLUMN', access => 'SELECT', principal => 'analyst',
                |  database => 'sales', table => 'renamed_column_lifecycle',
                |  excluded_column_names => array('private_secret'))
                |""".stripMargin)
    spark.sql("ALTER TABLE paimon.sales.renamed_column_lifecycle DROP COLUMN private_secret")
    checkAnswer(
      spark.sql(
        "CALL sys.list_permissions(resource_type => 'COLUMN', database => 'sales', " +
          "table => 'renamed_column_lifecycle', principal => 'analyst')"),
      Nil
    )

    grantTablePermission("renamed_column_lifecycle", "analyst")
    spark.sql("DROP TABLE paimon.sales.renamed_column_lifecycle")
    spark.sql("CREATE TABLE paimon.sales.renamed_column_lifecycle (id INT)")
    checkAnswer(
      spark.sql(
        "CALL sys.list_permissions(resource_type => 'TABLE', database => 'sales', " +
          "table => 'renamed_column_lifecycle', principal => 'analyst')"),
      Nil
    )
  }

  test("create, reject duplicates, apply and idempotently drop table data policies") {
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

    val duplicate = intercept[Exception] {
      spark
        .sql(s"""CALL sys.create_policy(
                |  database => 'sales',
                |  table => 'orders',
                |  policy_type => 'ROW_FILTER',
                |  principal => 'analyst',
                |  predicate_json => ${sqlLiteral(emeaFilter)})
                |""".stripMargin)
        .collect()
    }
    assertThat(duplicate.getMessage).contains("already exists")
    val unchanged = spark
      .sql("""CALL sys.list_policies(
             |  database => 'sales',
             |  table => 'orders',
             |  policy_type => 'ROW_FILTER',
             |  principal => 'analyst')
             |""".stripMargin)
      .head()
    assertThat(unchanged.getString(4)).contains("APAC").doesNotContain("EMEA")

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
      Row(1, "APAC", "APAC-masked")
    )
    checkAnswer(
      spark.sql("SELECT email FROM paimon.sales.orders"),
      Row("APAC-masked")
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

  test("create row-filter policies from SQL and enforce the complete condition") {
    restCatalogServer.setQueryPrincipals(Collections.singleton("analyst"))
    withTable("paimon.sales.sql_filter_orders") {
      spark.sql("""CREATE TABLE paimon.sales.sql_filter_orders (
                  |  id INT, region STRING, email STRING, amount BIGINT,
                  |  order_date DATE, `quoted id` INT)
                  |TBLPROPERTIES ('query-auth.enabled' = 'true')
                  |""".stripMargin)
      val values = """(1, 'APAC', 'a@example.com', 10, DATE '2026-10-28', 1),
                     |(2, 'EMEA', NULL, 20, DATE '2026-10-29', 2),
                     |(3, 'APAC', NULL, 30, DATE '2026-10-29', 3),
                     |(4, NULL, 'd@example.com', 40, DATE '2026-10-30', 4)
                     |""".stripMargin
      spark.sql(s"INSERT INTO paimon.sales.sql_filter_orders VALUES $values")

      val conditions = Seq(
        "REGION = 'APAC'",
        "id >= 2 AND (region IN ('APAC', 'EMEA') OR region IS NULL)",
        "(id <= 2 AND email IS NOT NULL) OR (id > 2 AND email IS NULL)",
        "NOT (region = 'APAC')",
        "region NOT IN ('EMEA', NULL)",
        "region <=> NULL OR region <=> 'APAC'",
        "amount = 20 AND order_date = DATE '2026-10-29'",
        "id = CAST(floor(pi()) AS INT)",
        "id >= CAST(1 + 1 AS INT)",
        "true",
        "false"
      ) ++ (if (org.apache.spark.SPARK_VERSION >= "3.3") Seq("`quoted id` = 2") else Nil)
      conditions.foreach {
        condition =>
          val expected = spark
            .sql(s"""SELECT id FROM VALUES $values
                    |AS raw(id, region, email, amount, order_date, `quoted id`)
                    |WHERE $condition
                    |""".stripMargin)
            .collect()
            .toSeq
          checkAnswer(
            spark.sql(s"""CALL sys.create_policy(
                         |  database => 'sales', table => 'sql_filter_orders',
                         |  policy_type => 'ROW_FILTER', principal => 'analyst',
                         |  filter_sql => ${sqlLiteral(condition)})
                         |""".stripMargin),
            Row(true)
          )
          try {
            checkAnswer(spark.sql("SELECT id FROM paimon.sales.sql_filter_orders"), expected)
            val stored = spark
              .sql("CALL sys.list_policies(database => 'sales', table => 'sql_filter_orders')")
              .head()
              .getString(4)
            assertThat(stored).contains("\"kind\":")
          } finally {
            spark.sql(
              "CALL sys.drop_policy(database => 'sales', table => 'sql_filter_orders', " +
                "policy_type => 'ROW_FILTER', principal => 'analyst')")
          }
      }
    }
  }

  Seq("FLOAT", "DOUBLE").foreach {
    dataType =>
      test(s"SQL policy filters preserve $dataType signed-zero semantics") {
        restCatalogServer.setQueryPrincipals(Collections.singleton("analyst"))
        withTable("paimon.sales.sql_float_policy") {
          spark.sql(s"""CREATE TABLE paimon.sales.sql_float_policy (id INT, v $dataType)
                       |TBLPROPERTIES ('query-auth.enabled' = 'true')
                       |""".stripMargin)
          def literal(value: String): String = s"CAST('$value' AS $dataType)"
          val values =
            Seq("-Infinity", "-1.0", "-0.0", "0.0", "1.0", "Infinity", "NaN").zipWithIndex
              .map { case (value, index) => s"(${index + 1}, ${literal(value)})" }
              .mkString(", ") + s", (8, CAST(NULL AS $dataType))"
          spark.sql(s"INSERT INTO paimon.sales.sql_float_policy VALUES $values")

          val zeroLiterals = Seq(literal("0.0"), literal("-0.0"))
          val comparisons = for {
            zero <- zeroLiterals
            operator <- Seq("=", "<>", "<", "<=", ">", ">=", "<=>")
          } yield s"v $operator $zero"
          val combinations = zeroLiterals.flatMap {
            zero =>
              Seq(
                s"v IN ($zero)",
                s"v NOT IN ($zero)",
                s"v IN ($zero, ${literal("1.0")}, CAST(NULL AS $dataType))",
                s"v NOT IN ($zero, CAST(NULL AS $dataType))",
                s"NOT (v <=> $zero)",
                s"v >= $zero AND v <= $zero",
                s"NOT (v >= $zero AND v <= $zero)",
                s"v >= $zero AND v <= ${literal("1.0")}",
                s"v >= ${literal("-1.0")} AND v <= $zero",
                s"NOT (v >= $zero AND v <= ${literal("1.0")})",
                s"NOT (v >= ${literal("-1.0")} AND v <= $zero)",
                s"NOT (v = $zero OR id = 5)",
                s"(v < $zero AND id <> 2) OR (v = $zero AND id = 3)"
              )
          }
          // Keep enough literals to exercise IN leaves rather than expanded equality predicates.
          val largeSet = (-10 to 10).map(value => literal(value.toString)).mkString(", ")
          val conditions = comparisons ++ combinations ++ Seq(
            s"v IN ($largeSet)",
            s"v NOT IN ($largeSet)",
            s"v IN (${zeroLiterals.mkString(", ")})",
            s"v >= ${literal("0.0")} AND v <= ${literal("-0.0")}",
            s"v >= ${literal("-0.0")} AND v <= ${literal("0.0")}",
            s"NOT (v >= ${literal("0.0")} AND v <= ${literal("-0.0")})",
            s"NOT (v >= ${literal("-0.0")} AND v <= ${literal("0.0")})",
            s"v = ${literal("NaN")}"
          )
          conditions.foreach {
            condition =>
              withClue(s"$dataType policy: $condition: ") {
                val expected = spark
                  .sql(s"SELECT id FROM VALUES $values AS raw(id, v) WHERE $condition")
                  .collect()
                  .toSeq
                checkSqlPolicy("sql_float_policy", condition, expected)
              }
          }

          // Predicate JSON keeps Paimon's native floating-point ordering.
          val nativePredicate = s"""{"kind":"LEAF","transform":{"name":"FIELD_REF",
                                   |"fieldRef":{"index":1,"name":"v","type":"$dataType"}},
                                   |"function":"NOT_EQUAL","literals":[0]}""".stripMargin
          spark.sql(s"""CALL sys.create_policy(
                       |  database => 'sales', table => 'sql_float_policy',
                       |  policy_type => 'ROW_FILTER', principal => 'analyst',
                       |  predicate_json => ${sqlLiteral(nativePredicate)})
                       |""".stripMargin)
          checkAnswer(
            spark.sql("SELECT id FROM paimon.sales.sql_float_policy"),
            Seq(Row(1), Row(2), Row(3), Row(5), Row(6), Row(7)))
        }
      }
  }

  Seq(DataTypes.TIMESTAMP(9), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)).foreach {
    timestampType =>
      test(s"SQL policy filters preserve ${timestampType.asSQLString()} microsecond semantics") {
        withSparkSQLConf(
          "spark.sql.session.timeZone" -> "UTC",
          "spark.paimon.legacy-timestamp-mapping.enabled" -> "false") {
          assume(!treatPaimonTimestampTypeAsSparkTimestampType())
          restCatalogServer.setQueryPrincipals(Collections.singleton("analyst"))
          withTable("paimon.sales.sql_timestamp_policy") {
            val identifier = Identifier.create("sales", "sql_timestamp_policy")
            paimonCatalog.createTable(
              identifier,
              Schema
                .newBuilder()
                .column("id", DataTypes.INT())
                .column("t", timestampType)
                .column("nested", DataTypes.ROW(DataTypes.FIELD(0, "t", timestampType)))
                .option("query-auth.enabled", "true")
                .build(),
              false
            )
            val timestamps = Seq(
              "1969-12-31T23:59:59.999999900",
              "1970-01-01T00:00:00.000000100",
              "1970-01-01T00:00:00",
              "1970-01-01T00:00:00.000001900",
              "2025-12-31T23:59:59.999999999",
              "2026-01-01T00:00:00.000000100",
              "2026-01-01T00:00:00.123456789",
              "2026-01-01T00:00:00.123456000",
              "9999-12-31T23:59:59.999999999",
              null
            )
            // Write through the SDK so Spark cannot truncate nanoseconds before storage.
            val builder = paimonCatalog.getTable(identifier).newBatchWriteBuilder()
            val writer = builder.newWrite()
            val commit = builder.newCommit()
            try {
              timestamps.zipWithIndex.foreach {
                case (value, index) =>
                  val timestamp =
                    if (value == null) null
                    else Timestamp.fromLocalDateTime(LocalDateTime.parse(value))
                  writer.write(
                    GenericRow.of(Int.box(index + 1), timestamp, GenericRow.of(timestamp)))
              }
              commit.commit(writer.prepareCommit())
            } finally {
              writer.close()
              commit.close()
            }

            val sparkType = SparkTypeUtils.fromPaimonType(timestampType).sql
            def literal(value: String): String = s"CAST('$value' AS $sparkType)"
            val values = timestamps.zipWithIndex
              .map {
                case (value, index) =>
                  val timestamp = if (value == null) s"CAST(NULL AS $sparkType)" else literal(value)
                  s"(${index + 1}, $timestamp)"
              }
              .mkString(", ")
            val boundaries = Seq(
              "1969-12-31T23:59:59.999999",
              "1970-01-01T00:00:00",
              "2026-01-01T00:00:00.123456",
              "9999-12-31T23:59:59.999999").map(literal)
            val comparisons = for {
              boundary <- boundaries
              operator <- Seq("=", "<>", "<", "<=", ">", ">=", "<=>")
            } yield s"t $operator $boundary"
            val boundary = boundaries(1)
            val conditions = comparisons ++ Seq(
              s"t IN ($boundary, ${boundaries(2)})",
              s"t NOT IN ($boundary, ${boundaries(2)})",
              s"t NOT IN ($boundary, CAST(NULL AS $sparkType))",
              s"NOT (t <=> $boundary)",
              s"t >= $boundary AND t <= ${boundaries(2)}",
              s"NOT (t >= $boundary AND t <= ${boundaries(2)})",
              s"t = $boundary OR t IS NULL",
              "t IS NULL",
              "t IS NOT NULL"
            )
            conditions.foreach {
              condition =>
                withClue(s"${timestampType.asSQLString()} policy: $condition: ") {
                  val expected = spark
                    .sql(s"SELECT id FROM VALUES $values AS raw(id, t) WHERE $condition")
                    .collect()
                    .toSeq
                  checkSqlPolicy("sql_timestamp_policy", condition, expected)
                }
            }

            val error = intercept[IllegalArgumentException] {
              spark.sql(s"""CALL sys.create_policy(
                           |  database => 'sales', table => 'sql_timestamp_policy',
                           |  policy_type => 'ROW_FILTER', principal => 'analyst',
                           |  filter_sql => ${sqlLiteral(s"nested.t <> $boundary")})
                           |""".stripMargin)
            }
            assertThat(error.getMessage).contains("nested timestamp fields")
            checkSqlPolicy("sql_timestamp_policy", "nested.t IS NULL", Seq(Row(10)))
          }
        }
      }
  }

  test("SQL policy filters reject timestamp value comparisons with legacy mapping") {
    withSparkSQLConf("spark.paimon.legacy-timestamp-mapping.enabled" -> "true") {
      withTimeZone("America/Los_Angeles") {
        restCatalogServer.setQueryPrincipals(Collections.singleton("analyst"))
        withTable("paimon.sales.legacy_sql_timestamp_policy") {
          val identifier = Identifier.create("sales", "legacy_sql_timestamp_policy")
          val timestampType = DataTypes.TIMESTAMP(9)
          paimonCatalog.createTable(
            identifier,
            Schema
              .newBuilder()
              .column("id", DataTypes.INT())
              .column("t", timestampType)
              .column("t_micros", DataTypes.TIMESTAMP(6))
              .column("t_ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9))
              .column("v", DataTypes.DOUBLE())
              .column("nested", DataTypes.ROW(DataTypes.FIELD(0, "t", timestampType)))
              .option("query-auth.enabled", "true")
              .build(),
            false
          )
          // The DST gap makes the first two stored local times appear identical in Spark.
          val values = Seq(
            "2026-03-08T02:30:00.000000100",
            "2026-03-08T03:30:00.000000100",
            "2026-03-08T04:30:00.000000100",
            null)
          val floats = Seq(Double.box(-0.0d), Double.box(0.0d), Double.box(1.0d), null)
          val builder = paimonCatalog.getTable(identifier).newBatchWriteBuilder()
          val writer = builder.newWrite()
          val commit = builder.newCommit()
          try {
            values.zipWithIndex.foreach {
              case (value, index) =>
                val dateTime = if (value == null) null else LocalDateTime.parse(value)
                val timestamp =
                  if (dateTime == null) null else Timestamp.fromLocalDateTime(dateTime)
                val micros =
                  if (dateTime == null) null
                  else Timestamp.fromLocalDateTime(dateTime.withNano(0))
                writer.write(
                  GenericRow.of(
                    Int.box(index + 1),
                    timestamp,
                    micros,
                    timestamp,
                    floats(index),
                    GenericRow.of(timestamp)))
            }
            commit.commit(writer.prepareCommit())
          } finally {
            writer.close()
            commit.close()
          }
          checkAnswer(
            spark.sql("SELECT id, CAST(t AS STRING) FROM paimon.sales.legacy_sql_timestamp_policy"),
            Seq(
              Row(1, "2026-03-08 03:30:00"),
              Row(2, "2026-03-08 03:30:00"),
              Row(3, "2026-03-08 04:30:00"),
              Row(4, null))
          )

          val boundary = "TIMESTAMP '2026-03-08 03:30:00'"
          val comparisons = for {
            column <- Seq("t", "t_micros", "t_ltz", "nested.t")
            operator <- Seq("=", "<>", "<", "<=", ">", ">=", "<=>")
          } yield s"$column $operator $boundary"
          val conditions = comparisons ++ Seq(
            s"t IN ($boundary)",
            s"t NOT IN ($boundary)",
            s"NOT (t <=> $boundary)",
            s"t >= $boundary AND t <= $boundary",
            s"id = 3 AND t <> $boundary",
            s"id = 3 OR t <> $boundary"
          )
          conditions.foreach {
            condition =>
              withClue(s"Legacy timestamp policy: $condition: ") {
                val error = intercept[IllegalArgumentException] {
                  spark
                    .sql(s"""CALL sys.create_policy(
                            |  database => 'sales', table => 'legacy_sql_timestamp_policy',
                            |  policy_type => 'ROW_FILTER', principal => 'analyst',
                            |  filter_sql => ${sqlLiteral(condition)})
                            |""".stripMargin)
                    .collect()
                }
                assertThat(error.getMessage).contains("legacy timestamp mapping")
                checkAnswer(
                  spark.sql("CALL sys.list_policies(database => 'sales', " +
                    "table => 'legacy_sql_timestamp_policy')"),
                  Nil)
              }
          }
          checkSqlPolicy("legacy_sql_timestamp_policy", "id >= 2", Seq(Row(2), Row(3), Row(4)))
          checkSqlPolicy("legacy_sql_timestamp_policy", "t IS NULL", Seq(Row(4)))
          checkSqlPolicy("legacy_sql_timestamp_policy", "t <=> NULL", Seq(Row(4)))
          checkSqlPolicy(
            "legacy_sql_timestamp_policy",
            "t IS NOT NULL",
            Seq(Row(1), Row(2), Row(3)))
          checkSqlPolicy("legacy_sql_timestamp_policy", "nested.t IS NULL", Seq(Row(4)))
          checkSqlPolicy("legacy_sql_timestamp_policy", "v <> CAST(0 AS DOUBLE)", Seq(Row(3)))

          val nativePredicate = JsonSerdeUtil.toFlatJson(
            LeafPredicate.of(
              new FieldTransform(new FieldRef(1, "t", timestampType)),
              NotEqual.INSTANCE,
              Collections.singletonList(Timestamp.fromLocalDateTime(LocalDateTime.parse(values(1))))
            ))
          spark.sql(s"""CALL sys.create_policy(
                       |  database => 'sales', table => 'legacy_sql_timestamp_policy',
                       |  policy_type => 'ROW_FILTER', principal => 'analyst',
                       |  predicate_json => ${sqlLiteral(nativePredicate)})
                       |""".stripMargin)
          checkAnswer(
            spark.sql("SELECT id FROM paimon.sales.legacy_sql_timestamp_policy"),
            Seq(Row(1), Row(3)))
        }
      }
    }
  }

  test("SQL policy filters reject locale-sensitive case conversion after constant folding") {
    restCatalogServer.setQueryPrincipals(Collections.singleton("analyst"))
    withTable("paimon.sales.sql_locale_policy") {
      spark.sql("""CREATE TABLE paimon.sales.sql_locale_policy (id INT, region STRING)
                  |TBLPROPERTIES ('query-auth.enabled' = 'true')
                  |""".stripMargin)
      // The non-ASCII suffix forces Spark 3's locale-sensitive fallback for the entire string.
      val values = Seq("I\u00e9", "i\u00e9", "APAC", "apac").zipWithIndex
        .map { case (value, index) => s"(${index + 1}, ${sqlLiteral(value)})" }
        .mkString(", ") + ", (5, CAST(NULL AS STRING))"
      spark.sql(s"INSERT INTO paimon.sales.sql_locale_policy VALUES $values")
      val turkishLower = sqlLiteral("\u0131\u00e9")
      val turkishUpper = sqlLiteral("\u0130\u00c9")
      val previousLocale = Locale.getDefault
      try {
        Seq(Locale.ROOT, new Locale("tr", "TR")).foreach {
          locale =>
            Locale.setDefault(locale)
            // Spark 3 uses the JVM locale for non-ASCII case conversion; Spark 4 has other paths.
            if (locale.getLanguage == "tr" && org.apache.spark.SPARK_VERSION < "4.0") {
              Seq(
                s"id = 1 AND lower(region) <> $turkishLower",
                s"id = 2 AND upper(region) <> $turkishUpper").foreach {
                condition =>
                  checkAnswer(
                    spark.sql(s"SELECT id FROM VALUES $values AS raw(id, region) WHERE $condition"),
                    Nil)
              }
            }
            val conditions = Seq(
              s"lower(region) <> $turkishLower",
              s"upper(region) <> $turkishUpper",
              "lower(region) = 'apac'",
              "upper(region) IN ('APAC')",
              "lower(region) IS NULL",
              "upper(region) IS NOT NULL",
              "NOT (lower(region) = 'apac')",
              "id = 3 AND lower(region) = 'apac'",
              "id = 3 OR upper(region) = 'APAC'",
              "length(lower(region)) = 4",
              "lower(lower(region)) = 'apac'",
              "upper(concat(region, 'x')) = 'APACX'"
            )
            conditions.foreach {
              condition =>
                withClue(s"$locale policy: $condition: ") {
                  val error = intercept[IllegalArgumentException] {
                    spark
                      .sql(s"""CALL sys.create_policy(
                              |  database => 'sales', table => 'sql_locale_policy',
                              |  policy_type => 'ROW_FILTER', principal => 'analyst',
                              |  filter_sql => ${sqlLiteral(condition)})
                              |""".stripMargin)
                      .collect()
                  }
                  assertThat(error.getMessage).contains(
                    "LOWER or UPPER on non-constant expressions")
                  checkAnswer(
                    spark.sql("CALL sys.list_policies(database => 'sales', " +
                      "table => 'sql_locale_policy')"),
                    Nil)
                }
            }

            // Only the folded literal is persisted, so its conversion runs in Spark at creation.
            Seq(
              "region = lower('APAC')",
              "region = upper('apac')",
              s"region = lower(${sqlLiteral("I\u00e9")})",
              s"region = upper(${sqlLiteral("i\u00e9")})",
              "region IS NULL"
            ).foreach {
              condition =>
                val expected = spark
                  .sql(s"SELECT id FROM VALUES $values AS raw(id, region) WHERE $condition")
                  .collect()
                  .toSeq
                checkSqlPolicy("sql_locale_policy", condition, expected)
            }
        }

        // Native Predicate JSON continues to use Paimon's Locale.ROOT conversion.
        val nativePredicate = JsonSerdeUtil.toFlatJson(
          LeafPredicate.of(
            new LowerTransform(
              Collections.singletonList(new FieldRef(1, "region", DataTypes.STRING()))),
            Equal.INSTANCE,
            Collections.singletonList(BinaryString.fromString("i\u00e9"))
          ))
        spark.sql(s"""CALL sys.create_policy(
                     |  database => 'sales', table => 'sql_locale_policy',
                     |  policy_type => 'ROW_FILTER', principal => 'analyst',
                     |  predicate_json => ${sqlLiteral(nativePredicate)})
                     |""".stripMargin)
        checkAnswer(spark.sql("SELECT id FROM paimon.sales.sql_locale_policy"), Seq(Row(1), Row(2)))
      } finally {
        // Locale is process-wide; restore it before other tests run.
        Locale.setDefault(previousLocale)
      }
    }
  }

  test("SQL policy filters reject invalid and partially convertible conditions") {
    withTable("paimon.sales.rejected_sql_filters") {
      spark.sql("""CREATE TABLE paimon.sales.rejected_sql_filters (
                  |  id INT, region STRING, `quoted id` INT)
                  |TBLPROPERTIES ('query-auth.enabled' = 'true')
                  |""".stripMargin)
      val conditions = Seq(
        "id = 1 AND abs(id) = 1",
        "id = 1 OR abs(id) = 1",
        "id = 1 OR (id = 2 AND abs(id) = 1)",
        "id = 1 AND (region = 'APAC' OR abs(id) = 1)",
        "missing = 1",
        "id",
        "id =",
        "id = 1 AND rand() < 0.5",
        "id = (SELECT 1)"
      ) ++ (if (org.apache.spark.SPARK_VERSION < "3.3") Seq("`quoted id` = 2") else Nil)
      conditions.foreach {
        condition =>
          intercept[Exception] {
            spark
              .sql(s"""CALL sys.create_policy(
                      |  database => 'sales', table => 'rejected_sql_filters',
                      |  policy_type => 'ROW_FILTER', principal => 'analyst',
                      |  filter_sql => ${sqlLiteral(condition)})
                      |""".stripMargin)
              .collect()
          }
          checkAnswer(
            spark.sql(
              "CALL sys.list_policies(database => 'sales', " +
                "table => 'rejected_sql_filters')"),
            Nil)
      }
    }
  }

  test("SQL policy filters reject query context before constant folding") {
    withTable("paimon.sales.context_sql_filters") {
      spark.sql("""CREATE TABLE paimon.sales.context_sql_filters (id INT, region STRING)
                  |TBLPROPERTIES ('query-auth.enabled' = 'true')
                  |""".stripMargin)
      val conditions = Seq(
        "id = dayofmonth(current_date())",
        "id = year(current_timestamp())",
        "id = year(localtimestamp())",
        "region = current_user()",
        "region = current_database()",
        "region = current_catalog()",
        "region = current_timezone()"
      ) ++ (if (org.apache.spark.SPARK_VERSION >= "4.1") {
              Seq("current_time() IS NOT NULL")
            } else {
              Nil
            })
      conditions.foreach {
        condition =>
          val error = intercept[IllegalArgumentException] {
            spark
              .sql(s"""CALL sys.create_policy(
                      |  database => 'sales', table => 'context_sql_filters',
                      |  policy_type => 'ROW_FILTER', principal => 'analyst',
                      |  filter_sql => ${sqlLiteral(condition)})
                      |""".stripMargin)
              .collect()
          }
          assertThat(error.getMessage).contains("depend on the current query context")
      }
      checkAnswer(
        spark.sql("CALL sys.list_policies(database => 'sales', table => 'context_sql_filters')"),
        Nil)
    }
  }

  test("SQL policy filter arguments preserve the predicate JSON interface") {
    withTable("paimon.sales.sql_filter_arguments") {
      spark.sql("""CREATE TABLE paimon.sales.sql_filter_arguments (id INT)
                  |TBLPROPERTIES ('query-auth.enabled' = 'true')
                  |""".stripMargin)
      val predicate = sqlLiteral(intEqualsPredicate(0, "id", 1))
      Seq(
        ("ROW_FILTER", "", "exactly one of predicate_json or filter_sql"),
        ("ROW_FILTER", ", filter_sql => ' '", "exactly one of predicate_json or filter_sql"),
        (
          "ROW_FILTER",
          s", predicate_json => $predicate, filter_sql => 'id = 1'",
          "exactly one of predicate_json or filter_sql"),
        ("COLUMN_MASKING", ", filter_sql => 'id = 1'", "cannot specify filter_sql")
      ).foreach {
        case (policyType, arguments, message) =>
          val error = intercept[IllegalArgumentException] {
            spark
              .sql(s"""CALL sys.create_policy(
                      |  database => 'sales', table => 'sql_filter_arguments',
                      |  policy_type => '$policyType', principal => 'analyst'$arguments)
                      |""".stripMargin)
              .collect()
          }
          assertThat(error.getMessage).contains(message)
      }

      checkAnswer(
        spark.sql(
          s"CALL sys.create_policy('sales', 'sql_filter_arguments', " +
            s"'ROW_FILTER', 'analyst', $predicate)"),
        Row(true)
      )
      assertThat(
        spark
          .sql("CALL sys.list_policies(database => 'sales', table => 'sql_filter_arguments')")
          .head()
          .getString(4)).contains("\"name\":\"id\"")
    }
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
    assertThat(missingColumn.getMessage).contains("Cannot resolve field 'unknown'")

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
      val pageToken = first.getString(7)
      assertThat(pageToken).isNotEmpty.isNotEqualTo("1")

      spark.sql(
        "CALL sys.drop_policy(database => 'sales', table => 'orders', " +
          "policy_type => 'ROW_FILTER', principal => 'first', if_exists => true)")

      val second = spark
        .sql(
          "CALL sys.list_policies(database => 'sales', table => 'orders', " +
            s"max_results => 1, page_token => ${sqlLiteral(pageToken)})")
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
    assertThat(renameColumn.getMessage).contains("Cannot resolve field 'region'")

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

  // Read without WHERE so a Spark residual cannot hide an incorrect authorization predicate.
  private def checkSqlPolicy(table: String, condition: String, expected: Seq[Row]): Unit = {
    checkAnswer(
      spark.sql(s"""CALL sys.create_policy(
                   |  database => 'sales', table => '$table',
                   |  policy_type => 'ROW_FILTER', principal => 'analyst',
                   |  filter_sql => ${sqlLiteral(condition)})
                   |""".stripMargin),
      Row(true)
    )
    try {
      checkAnswer(spark.sql(s"SELECT id FROM paimon.sales.$table"), expected)
    } finally {
      spark.sql(
        s"CALL sys.drop_policy(database => 'sales', table => '$table', " +
          "policy_type => 'ROW_FILTER', principal => 'analyst')")
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

  private def grantTablePermission(table: String, principal: String): Unit = {
    checkAnswer(
      spark.sql(s"""CALL sys.grant_permission(
                   |  resource_type => 'TABLE', access => 'SELECT', principal => '$principal',
                   |  database => 'sales', table => '$table')
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
    "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
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
