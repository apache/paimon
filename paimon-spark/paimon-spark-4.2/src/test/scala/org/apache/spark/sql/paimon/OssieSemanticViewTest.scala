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

package org.apache.spark.sql.paimon

import org.apache.paimon.catalog.{DelegateCatalog, Identifier}
import org.apache.paimon.management.SemanticViewManagement
import org.apache.paimon.rest.{RESTApi, RESTCatalog, RESTCatalogServer}
import org.apache.paimon.rest.auth.{AuthProvider, BearTokenAuthProvider}
import org.apache.paimon.rest.requests.UpsertSemanticViewRequest
import org.apache.paimon.rest.responses.{ConfigResponse, GetSemanticViewResponse}
import org.apache.paimon.spark.SparkCatalog
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
import org.apache.paimon.view.SemanticViewDefinition

import okhttp3.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.TimestampType

import java.nio.file.Files
import java.time.Instant
import java.util.{Collections, Locale}
import java.util.concurrent.ConcurrentHashMap

/** Real Spark queries, real REST client, and physical Paimon source files. */
class OssieSemanticViewTest extends QueryTest with SharedSparkSession {
  private val id = Identifier.create("sales", "order_metrics")
  private val directory = Files.createTempDirectory("paimon-ossie-").toFile
  private lazy val server = {
    val s = new SemanticServer(directory.toURI.toString)
    s.start()
    s
  }
  private def models: SemanticViewManagement = {
    val catalog = spark.sessionState.catalogManager.catalog("paimon").asInstanceOf[SparkCatalog]
    DelegateCatalog
      .rootCatalog(catalog.paimonCatalog())
      .asInstanceOf[RESTCatalog]
      .semanticViewManagement()
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
    .set("spark.sql.catalog.paimon.metastore", "rest")
    .set("spark.sql.catalog.paimon.uri", server.getUrl)
    .set("spark.sql.catalog.paimon.warehouse", "ossie-tests")
    .set("spark.sql.catalog.paimon.token.provider", "bear")
    .set("spark.sql.catalog.paimon.token", "test-token")
    .set("spark.sql.catalog.paimon.cache-enabled", "false")
    .set("spark.sql.extensions", classOf[PaimonSparkSessionExtensions].getName)
    // Spark 4.3 dual-runs the single-pass resolver in tests, and it cannot resolve Paimon views.
    .set("spark.sql.analyzer.singlePassResolver.dualRunWithLegacy", "false")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("CREATE DATABASE paimon.sales")
    spark.sql(
      "CREATE TABLE paimon.sales.orders (region_code STRING, amount DECIMAL(12, 2), " +
        "discount DECIMAL(12, 2)) TBLPROPERTIES ('bucket'='-1', 'file.format'='avro')")
    spark.sql(
      "INSERT INTO paimon.sales.orders VALUES ('CN', 100, 10), ('CN', 100, 20), ('SG', 80, 20)")
  }

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      server.shutdown()
      org.apache.spark.util.Utils.deleteRecursively(directory)
    }
  }

  private def register(yaml: String = model): Unit = {
    server.semanticStatus = 200
    server.denySource = false
    models.upsertSemanticView(id, new SemanticViewDefinition("ossie-yaml", yaml))
  }

  test("query dataset and model metrics with field/source shadowing and distinct aggregates") {
    register()
    checkAnswer(
      spark.sql(
        "SELECT region, MEASURE(revenue), MEASURE(gross), MEASURE(orders), " +
          "MEASURE(average), MEASURE(distinct_amounts), MEASURE(model_revenue) " +
          "FROM paimon.sales.order_metrics GROUP BY region"),
      Seq(
        Row("CN", BigDecimal(170), BigDecimal(200), 2L, BigDecimal(85), 1L, BigDecimal(170)),
        Row("SG", BigDecimal(60), BigDecimal(80), 1L, BigDecimal(60), 1L, BigDecimal(60))
      )
    )
    checkAnswer(
      spark.sql(
        "SELECT MEASURE(revenue), MEASURE(orders) " +
          "FROM paimon.sales.order_metrics WHERE region = 'CN'"),
      Seq(Row(BigDecimal(170), 2L)))
    checkAnswer(
      spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics"),
      Seq(Row(BigDecimal(230))))
    assert(server.authorizedSemanticReads > 0)
    checkAnswer(
      spark.sql(
        "SELECT MEASURE(revenue), MEASURE(orders) " +
          "FROM paimon.sales.order_metrics WHERE region = 'missing'"),
      Seq(Row(null, 0L)))
  }

  test("group Ossie measures by DATE_TRUNC and preserve Spark timestamp semantics") {
    withSQLConf("spark.sql.session.timeZone" -> "UTC") {
      spark.sql(
        "CREATE TABLE paimon.sales.date_trunc_orders " +
          "(order_date DATE, order_time TIMESTAMP_LTZ, amount BIGINT) " +
          "TBLPROPERTIES ('bucket' = '-1')")
      spark.sql("""INSERT INTO paimon.sales.date_trunc_orders VALUES
                  |  (DATE '2026-05-10', NULL, 10),
                  |  (DATE '2026-05-31', TIMESTAMP '2026-05-31 13:42:18.123456', 20),
                  |  (DATE '2026-06-01', TIMESTAMP '2026-06-01 00:00:00.987654', 7),
                  |  (NULL, NULL, 3)""".stripMargin)

      val units = Seq("year", "quarter", "month", "week", "day", "hour", "minute", "second")
      val timestampFields = units
        .map {
          unit =>
            s"""      - name: by_$unit
               |        expression:
               |          sql: date_trunc('${unit.toUpperCase(Locale.ROOT)}', order_time)""".stripMargin
        }
        .mkString("\n")
      register(s"""version: 0.3.0.dev0
                  |name: order_metrics
                  |datasets:
                  |  - name: orders
                  |    source: date_trunc_orders
                  |    fields:
                  |      - name: order_month
                  |        expression:
                  |          sql: DATE_TRUNC('month', order_date)
                  |$timestampFields
                  |    metrics:
                  |      - name: revenue
                  |        expression:
                  |          sql: SUM(source.amount)
                  |""".stripMargin)

      val monthly = spark.sql(
        "SELECT order_month, MEASURE(revenue) AS revenue " +
          "FROM paimon.sales.order_metrics GROUP BY order_month")
      assert(monthly.schema("order_month").dataType == TimestampType)
      checkAnswer(
        monthly.selectExpr("CAST(order_month AS STRING)", "revenue"),
        Seq(Row("2026-05-01 00:00:00", 30L), Row("2026-06-01 00:00:00", 7L), Row(null, 3L)))

      val columns = units.map(unit => s"by_$unit")
      val grouping = columns.mkString(", ")
      val timestamps = spark.sql(
        s"SELECT $grouping, MEASURE(revenue) AS revenue " +
          s"FROM paimon.sales.order_metrics GROUP BY $grouping")
      assert(columns.forall(name => timestamps.schema(name).dataType == TimestampType))
      val formatted = columns.map(name => s"CAST($name AS STRING)") :+ "revenue"
      checkAnswer(
        timestamps.selectExpr(formatted: _*),
        Seq(
          Row(
            "2026-01-01 00:00:00",
            "2026-04-01 00:00:00",
            "2026-05-01 00:00:00",
            "2026-05-25 00:00:00",
            "2026-05-31 00:00:00",
            "2026-05-31 13:00:00",
            "2026-05-31 13:42:00",
            "2026-05-31 13:42:18",
            20L
          ),
          Row(
            "2026-01-01 00:00:00",
            "2026-04-01 00:00:00",
            "2026-06-01 00:00:00",
            "2026-06-01 00:00:00",
            "2026-06-01 00:00:00",
            "2026-06-01 00:00:00",
            "2026-06-01 00:00:00",
            "2026-06-01 00:00:00",
            7L
          ),
          Row.fromSeq(Seq.fill(columns.size)(null) :+ 13L)
        )
      )
    }
  }

  test("reject TIMESTAMP_NTZ inputs to Ossie DATE_TRUNC during analysis") {
    spark.sql(
      "CREATE TABLE paimon.sales.local_orders (order_time TIMESTAMP_NTZ, amount BIGINT) " +
        "TBLPROPERTIES ('bucket' = '-1')")
    spark.sql("""INSERT INTO paimon.sales.local_orders VALUES
                |  (TIMESTAMP_NTZ '2026-03-08 02:30:00', 10),
                |  (TIMESTAMP_NTZ '2026-03-08 03:30:00', 20)""".stripMargin)
    val yaml = """version: 0.3.0.dev0
                 |name: order_metrics
                 |datasets:
                 |  - name: orders
                 |    source: local_orders
                 |    fields:
                 |      - name: order_hour
                 |        expression:
                 |          sql: DATE_TRUNC('hour', order_time)
                 |    metrics:
                 |      - name: revenue
                 |        expression:
                 |          sql: SUM(source.amount)
                 |""".stripMargin
    register(yaml)
    val query = "SELECT order_hour, MEASURE(revenue) AS revenue " +
      "FROM paimon.sales.order_metrics GROUP BY order_hour"
    val message = "Ossie DATE_TRUNC does not support TIMESTAMP_NTZ inputs"
    Seq("UTC", "America/Los_Angeles").foreach {
      zone =>
        withSQLConf("spark.sql.session.timeZone" -> zone) {
          val error = intercept[IllegalArgumentException] {
            spark.sql(query).schema
          }
          assert(error.getMessage.contains(message), error.toString)
        }
    }

    register(yaml
      .replace("DATE_TRUNC('hour', order_time)", "order_time")
      .replace("SUM(source.amount)", "MAX(DATE_TRUNC('hour', COALESCE(source.order_time, NULL)))"))
    val error = intercept[IllegalArgumentException] {
      spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics").schema
    }
    assert(error.getMessage.contains(message), error.toString)
  }

  test("accept Spark timestamp strings in Ossie DATE_TRUNC") {
    withSQLConf("spark.sql.session.timeZone" -> "UTC") {
      register(model.replace("sql: region_code", "sql: DATE_TRUNC('month', '2026-06-01')"))
      val result = spark.sql(
        "SELECT region, MEASURE(gross) AS gross FROM paimon.sales.order_metrics GROUP BY region")
      assert(result.schema("region").dataType == TimestampType)
      checkAnswer(
        result.selectExpr("CAST(region AS STRING)", "gross"),
        Row("2026-06-01 00:00:00", BigDecimal(280)))
    }
  }

  test("DATE_TRUNC input validation preserves SQL UDF body semantics") {
    val session = spark.newSession()
    session.conf.set("spark.sql.path.enabled", "true")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    session.sql(
      "CREATE FUNCTION spark_catalog.default.floor(ts TIMESTAMP_NTZ) " +
        "RETURNS TIMESTAMP_LTZ RETURN date_trunc('day', ts)")
    try {
      session.sql("SET PATH = spark_catalog.default, system.builtin")
      val floor = "FLOOR(TIMESTAMP_NTZ '2026-06-12 10:11:12')"
      val truncate = "DATE_TRUNC('day', TIMESTAMP_LTZ '2026-06-12 10:11:12Z')"
      Seq(floor, s"COALESCE($floor, $truncate)", s"DATE_TRUNC('day', $floor)").foreach {
        expression =>
          register(model.replace("sql: region_code", s"sql: $expression"))
          val result = session.sql(
            "SELECT region, MEASURE(gross) AS gross FROM paimon.sales.order_metrics GROUP BY region")
          assert(result.schema("region").dataType == TimestampType)
          checkAnswer(
            result.selectExpr("CAST(region AS STRING)", "gross"),
            Row("2026-06-12 00:00:00", BigDecimal(280)))
      }
      register(model.replace("sql: SUM(amount)", s"sql: MAX($floor)"))
      checkAnswer(
        session.sql("SELECT CAST(MEASURE(revenue) AS STRING) FROM paimon.sales.order_metrics"),
        Row("2026-06-12 00:00:00"))
    } finally {
      session.sql("DROP FUNCTION spark_catalog.default.floor")
    }
  }

  test("resolve Ossie DATE_TRUNC to the Spark builtin regardless of the SQL path") {
    val session = spark.newSession()
    session.conf.set("spark.sql.path.enabled", "true")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    session.sql(
      "CREATE FUNCTION spark_catalog.default.date_trunc(unit STRING, ts TIMESTAMP_LTZ) " +
        "RETURNS STRING RETURN 'shadow'")
    try {
      session.sql("SET PATH = spark_catalog.default, system.builtin")
      val timestamp = "TIMESTAMP_LTZ '2026-06-12 10:11:12Z'"
      checkAnswer(session.sql(s"SELECT date_trunc('month', $timestamp)"), Row("shadow"))

      register(model.replace("sql: region_code", s"sql: DaTe_TrUnC('month', $timestamp)"))
      val query =
        "SELECT region, MEASURE(gross) AS gross FROM paimon.sales.order_metrics GROUP BY region"
      val result = session.sql(query)
      checkAnswer(
        result.selectExpr("CAST(region AS STRING)", "gross"),
        Row("2026-06-01 00:00:00", BigDecimal(280)))
      assert(result.schema("region").dataType == TimestampType)

      register(
        model.replace(
          "sql: region_code",
          "sql: DATE_TRUNC('month', TIMESTAMP_NTZ '2026-06-12 10:11:12')"))
      val error = intercept[IllegalArgumentException] {
        session.sql(query).schema
      }
      assert(error.getMessage.contains("Ossie DATE_TRUNC does not support TIMESTAMP_NTZ inputs"))
    } finally {
      session.sql("DROP FUNCTION spark_catalog.default.date_trunc")
    }
  }

  Seq("TIMESTAMP_LTZ", "TIMESTAMP_NTZ").foreach {
    timestampType =>
      test(s"preserve Ossie DATE_TRUNC literal instants with $timestampType as the default") {
        withSQLConf(
          "spark.sql.timestampType" -> timestampType,
          "spark.sql.session.timeZone" -> "America/Los_Angeles") {
          val timestamp = "CASE WHEN region_code = 'CN' " +
            "THEN TIMESTAMP_LTZ '2026-11-01 01:35:00.123456-07:00' " +
            "ELSE TIMESTAMP_LTZ '2026-11-01 01:35:00.654321-08:00' END"
          register(model.replace("sql: region_code", s"sql: DATE_TRUNC('hour', $timestamp)"))
          val result = spark.sql(
            "SELECT region, MEASURE(gross) AS gross FROM paimon.sales.order_metrics GROUP BY region")
          assert(result.schema("region").dataType == TimestampType)
          checkAnswer(
            result.selectExpr("unix_micros(region)", "gross"),
            Seq(
              Row(Instant.parse("2026-11-01T08:00:00Z").getEpochSecond * 1000000L, BigDecimal(200)),
              Row(Instant.parse("2026-11-01T09:00:00Z").getEpochSecond * 1000000L, BigDecimal(80))
            )
          )
        }
      }
  }

  test("reject invalid DATE_TRUNC units and arguments in Ossie models") {
    val date = "DATE '2026-06-01'"
    Seq(
      s"DATE_TRUNC('mont', $date)" -> "Unsupported Ossie DATE_TRUNC unit: mont",
      s"DATE_TRUNC('MM', $date)" -> "Unsupported Ossie DATE_TRUNC unit: MM",
      s"DATE_TRUNC('millisecond', $date)" -> "Unsupported Ossie DATE_TRUNC unit: millisecond",
      s"DATE_TRUNC('WEE\u212A', $date)" -> "Unsupported Ossie DATE_TRUNC unit: WEE\u212A",
      s"DATE_TRUNC(region_code, $date)" -> "Ossie DATE_TRUNC expects a unit string literal",
      s"DATE_TRUNC(NULL, $date)" -> "Ossie DATE_TRUNC expects a unit string literal",
      "DATE_TRUNC('month')" -> "Ossie DATE_TRUNC expects a unit string literal"
    ).foreach {
      case (expression, message) =>
        register(model.replace("sql: region_code", s"sql: $expression"))
        val error = intercept[IllegalArgumentException] {
          spark.sql("SELECT region FROM paimon.sales.order_metrics").collect()
        }
        assert(error.getMessage.contains(message), error.toString)
    }
  }

  test("public field names cannot shadow private metric input aliases") {
    Seq("__ossie_f1", "__ossie_0_f1").foreach {
      name =>
        register(model.replace("name: region", s"name: $name"))
        checkAnswer(
          spark.sql(
            s"SELECT MEASURE(revenue) FROM paimon.sales.order_metrics " +
              s"WHERE $name = 'CN'"),
          Seq(Row(BigDecimal(170))))
    }
  }

  test("preserve FILTER, quoted identifiers, true division, and half-away-from-zero rounding") {
    register(
      model
        .replace("sql: SUM(amount)", "sql: ROUND(SUM(amount) / COUNT(*), 0)")
        .replace(
          "sql: COUNT(DISTINCT source.amount)",
          "sql: COUNT(DISTINCT source.amount) FILTER (WHERE region = 'CN')")
        .replace(
          "sql: AVG(amount)",
          "sql: AVG(CASE WHEN \"region\" = 'CN' THEN \"amount\" ELSE NULL END)")
        .replace("sql: SUM(source.amount)", "sql: ROUND(MIN(CAST(-2.5 AS DECIMAL(3, 1))), 0)")
        .replace("sql: SUM(orders.amount)", "sql: COUNT(*) / COUNT(DISTINCT orders.region)"))
    checkAnswer(
      spark.sql("SELECT MEASURE(revenue), MEASURE(distinct_amounts), " +
        "MEASURE(average), MEASURE(gross), MEASURE(model_revenue) FROM paimon.sales.order_metrics"),
      Seq(Row(BigDecimal(77), 1L, BigDecimal(85), BigDecimal(-3), 1.5d))
    )
  }

  test("new analysis sees REST replacement and is independent of the current database") {
    register()
    checkAnswer(
      spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics"),
      Seq(Row(BigDecimal(230))))
    register(model.replace("sql: SUM(amount)", "sql: MAX(amount)"))
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      checkAnswer(
        spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics"),
        Seq(Row(BigDecimal(90))))
    }
    checkAnswer(spark.sql("SELECT SUM(amount) FROM paimon.sales.orders"), Seq(Row(BigDecimal(280))))
    spark.sql(
      "CREATE OR REPLACE VIEW paimon.sales.ordinary AS SELECT region_code FROM paimon.sales.orders")
    checkAnswer(
      spark.sql("SELECT * FROM paimon.sales.ordinary"),
      Seq(Row("CN"), Row("CN"), Row("SG")))
    assert(spark.sql("DESCRIBE paimon.sales.orders").collect().exists(_.getString(0) == "amount"))
    spark.sql("CREATE OR REPLACE FUNCTION paimon.sales.add_one(x INT) RETURNS INT RETURN x + 1")
    checkAnswer(spark.sql("SELECT paimon.sales.add_one(4)"), Seq(Row(5)))
  }

  test("portable string literals keep backslashes and apostrophes under either Spark setting") {
    Seq(false, true).foreach {
      escaped =>
        withSQLConf("spark.sql.parser.escapedStringLiterals" -> escaped.toString) {
          Seq("C:\\new\\tab\\%\\path's", "O'Brien", "", "路径\\new").foreach {
            value =>
              val literal = "'" + value.replace("'", "''") + "'"
              register(
                model
                  .replace("sql: region_code", s"sql: |-\n            $literal")
                  .replace(
                    "sql: COUNT(DISTINCT source.amount)",
                    s"sql: COUNT(*) FILTER (WHERE region = $literal)"))
              checkAnswer(
                spark.sql("SELECT region, MEASURE(revenue), MEASURE(distinct_amounts) " +
                  "FROM paimon.sales.order_metrics GROUP BY region"),
                Seq(Row(value, BigDecimal(230), 3L))
              )
          }
          register(model
            .replace("sql: region_code", "sql: |-\n            CAST(DATE '2026-11-13' AS STRING)"))
          checkAnswer(
            spark.sql(
              "SELECT region, MEASURE(revenue) FROM paimon.sales.order_metrics GROUP BY region"),
            Seq(Row("2026-11-13", BigDecimal(230))))
          register(
            model.replace(
              "sql: region_code",
              "sql: |-\n            CASE WHEN \"region_code\" = 'CN' /* ' ignored */ " +
                "THEN 'O''Brien' -- ' ignored\n            ELSE '路径\\new' END"
            ))
          checkAnswer(
            spark.sql(
              "SELECT region, MEASURE(revenue) FROM paimon.sales.order_metrics GROUP BY region"),
            Seq(Row("O'Brien", BigDecimal(170)), Row("路径\\new", BigDecimal(60)))
          )
        }
    }
  }

  test("CTE aliases and lateral syntax cannot hide unsupported semantic joins") {
    register()
    Seq(
      "SELECT m.region, MEASURE(m.revenue) FROM paimon.sales.order_metrics m " +
        "JOIN LATERAL (SELECT 1 AS x) t ON true GROUP BY m.region",
      "WITH m AS (SELECT * FROM paimon.sales.order_metrics) " +
        "SELECT MEASURE(m.revenue) FROM m JOIN paimon.sales.orders o " +
        "ON m.region=o.region_code GROUP BY m.region",
      "WITH m AS (SELECT * FROM paimon.sales.order_metrics), n AS (SELECT * FROM m) " +
        "SELECT MEASURE(n.revenue) FROM n JOIN paimon.sales.orders o " +
        "ON n.region=o.region_code GROUP BY n.region",
      "WITH m AS (SELECT * FROM paimon.sales.order_metrics) " +
        "SELECT (SELECT MEASURE(m.revenue) FROM m JOIN paimon.sales.orders o " +
        "ON m.region=o.region_code) AS revenue"
    ).foreach {
      query =>
        val error = intercept[Exception](spark.sql(query).collect())
        assert(error.toString.contains("Joins against Ossie"), error.toString)
    }
    checkAnswer(
      spark.sql(
        "WITH m AS (SELECT MEASURE(revenue) AS revenue " +
          "FROM paimon.sales.order_metrics) SELECT revenue FROM m"),
      Seq(Row(BigDecimal(230))))
    checkAnswer(
      spark.sql(
        "WITH ordinary AS (SELECT o.region_code FROM paimon.sales.orders o " +
          "JOIN paimon.sales.orders p ON o.region_code=p.region_code) " +
          "SELECT MEASURE(revenue) FROM paimon.sales.order_metrics"),
      Seq(Row(BigDecimal(230)))
    )
  }

  test("portable SQL line comments end at CR, LF and CRLF") {
    Seq("\r", "\n", "\r\n").foreach {
      newline =>
        val expression = RESTApi.toJson("SUM(amount) -- a comment" + newline + " + COUNT(*)")
        register(model.replace("sql: SUM(amount)", "sql: " + expression))
        checkAnswer(
          spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics"),
          Seq(Row(BigDecimal(233))))
    }
  }

  test("ANSI quoted source columns retain backslashes and embedded double quotes") {
    val columns = Seq("tail\\", "back\\\"quote", "apostrophe'name")
    def quoted(name: String): String = "`" + name.replace("`", "``") + "`"
    val schema = columns.map(name => quoted(name) + " STRING").mkString(", ")
    spark.sql(
      "CREATE TABLE paimon.sales.quoted_columns (" + schema +
        ", amount DECIMAL(12, 2), discount DECIMAL(12, 2)) " +
        "TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet')")
    spark.sql("INSERT INTO paimon.sales.quoted_columns VALUES ('v0', 'v1', 'v2', 100, 10)")
    Seq(false, true).foreach {
      escaped =>
        withSQLConf("spark.sql.parser.escapedStringLiterals" -> escaped.toString) {
          columns.zipWithIndex.foreach {
            case (column, index) =>
              val expression = RESTApi.toJson("\"" + column.replace("\"", "\"\"") + "\"")
              register(
                model
                  .replace("source: orders", "source: quoted_columns")
                  .replace("sql: region_code", "sql: " + expression))
              checkAnswer(
                spark.sql(
                  "SELECT region, MEASURE(revenue) FROM paimon.sales.order_metrics GROUP BY region"),
                Seq(Row("v" + index, BigDecimal(90))))
          }
        }
    }
  }

  test("all semantic view write targets fail without changing the source") {
    register()
    val statements = Seq(
      "DELETE FROM paimon.sales.order_metrics",
      "UPDATE paimon.sales.order_metrics SET region='changed'",
      "MERGE INTO paimon.sales.order_metrics m USING (SELECT 'CN' AS region) s " +
        "ON m.region=s.region WHEN MATCHED THEN DELETE"
    )
    statements.foreach {
      statement =>
        intercept[Exception](spark.sql(statement).collect())
        checkAnswer(
          spark.sql("SELECT SUM(amount), COUNT(*) FROM paimon.sales.orders"),
          Seq(Row(BigDecimal(280), 3L)))
    }
  }

  test("preserve REST and source authorization errors") {
    register()
    Seq(403, 501).foreach {
      status =>
        server.semanticStatus = status
        checkAnswer(
          spark.sql("SELECT SUM(amount) FROM paimon.sales.orders"),
          Seq(Row(BigDecimal(280))))
        val error = intercept[Exception] {
          spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics").collect()
        }
        assert(error.toString.contains("semantic access failed"))
    }
    server.semanticStatus = 200
    server.denySource = true
    val error = intercept[Exception] {
      spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics").collect()
    }
    assert(error.toString.contains("source access denied"))
    server.denySource = false
    val missing = intercept[Exception] {
      spark.sql("SELECT * FROM paimon.sales.missing_model").collect()
    }
    assert(missing.toString.contains("TABLE_OR_VIEW_NOT_FOUND"))
  }

  test("reject unsupported models, joins, and invalid metric references") {
    Seq(
      model.replace("version: 0.3.0.dev0", "version: 0.1.1") -> "Invalid Ossie",
      model.replace("sql: SUM(amount)", "sql: SUM(discount)") -> "Unknown Ossie dataset field",
      model.replace(
        "sql: SUM(amount)",
        "sql: SUM(orders.amount)") -> "Invalid Ossie metric reference",
      model.replace("sql: SUM(amount)", "sql: SUM(gross)") -> "Unknown Ossie dataset field",
      model.replace("sql: SUM(amount)", "sql: MEDIAN(amount)") -> "Unsupported OSSIE_SQL_2026",
      model.replace("sql: SUM(amount)", "sql: SUM(amount) OVER ()") -> "Unsupported OSSIE_SQL_2026",
      model.replace("sql: region_code", "sql: |-\n            r'raw'") -> "ANSI string literals",
      model.replace("source: orders", "source: other.sales.orders") -> "same Paimon catalog",
      model.replace("source: orders", "source: order_metrics") -> "order_metrics"
    ).foreach {
      case (yaml, message) =>
        register(yaml)
        val error = intercept[Exception] {
          spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics").collect()
        }
        assert(error.toString.contains(message), error.toString)
    }
    register()
    val join = intercept[Exception] {
      spark
        .sql("SELECT MEASURE(m.revenue) FROM paimon.sales.order_metrics m " +
          "JOIN paimon.sales.orders o ON m.region = o.region_code GROUP BY m.region")
        .collect()
    }
    assert(join.toString.contains("Joins against Ossie"))
    val write = intercept[Exception] {
      spark.sql("INSERT INTO paimon.sales.order_metrics SELECT 'CN', 1, 2, 3, 4, 5, 6, 7").collect()
    }
    assert(
      write.toString.contains("read-only") ||
        write.toString.contains("TABLE_OR_VIEW_NOT_FOUND"),
      write.toString)
    intercept[Exception] {
      spark.readStream.table("paimon.sales.order_metrics")
    }
  }

  test(
    "source identifiers bind to the view catalog and database instead of the current namespace") {
    val manager = spark.sessionState.catalogManager
    val previous = manager.currentCatalog.name() +: manager.currentNamespace.toSeq
    spark.sql("CREATE DATABASE paimon.elsewhere")
    spark.sql(
      "CREATE TABLE paimon.elsewhere.orders " +
        "(region_code STRING, amount DECIMAL(12, 2), discount DECIMAL(12, 2)) " +
        "TBLPROPERTIES ('bucket'='-1', 'file.format'='avro')")
    spark.sql("INSERT INTO paimon.elsewhere.orders VALUES ('CN', 999, 0)")
    try {
      spark.sql("USE paimon.elsewhere")
      Seq("orders", "sales.orders", "paimon.sales.orders").foreach {
        source =>
          register(model.replace("source: orders", s"source: $source"))
          checkAnswer(
            spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics"),
            Seq(Row(BigDecimal(230))))
      }
    } finally {
      spark.sql("USE " + previous.map(p => "`" + p.replace("`", "``") + "`").mkString("."))
    }
  }

  test("analyzed queries retain their definition and queries preserve original metadata") {
    val original = model +
      "nonportable_extensions:\n  - vendor_name: unknown\n    data: {date: '2026-11-13', nested: [null, true]}\n"
    register(original)
    val writes = server.semanticWrites
    val retained = spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics")
    retained.queryExecution.analyzed
    checkAnswer(retained, Seq(Row(BigDecimal(230))))
    assert(models.getSemanticView(id).getDefinition.getContent == original)
    assert(server.semanticWrites == writes)
    register(model.replace("sql: SUM(amount)", "sql: MAX(amount)"))
    checkAnswer(retained, Seq(Row(BigDecimal(230))))
    checkAnswer(
      spark.sql("SELECT MEASURE(revenue) FROM paimon.sales.order_metrics"),
      Seq(Row(BigDecimal(90))))
  }

  private def model: String =
    """version: 0.3.0.dev0
      |name: order_metrics
      |datasets:
      |  - name: orders
      |    source: orders
      |    fields:
      |      - name: region
      |        expression:
      |          sql: region_code
      |      - name: amount
      |        expression:
      |          sql: amount - discount
      |    metrics:
      |      - name: revenue
      |        expression:
      |          sql: SUM(amount)
      |      - name: gross
      |        expression:
      |          sql: SUM(source.amount)
      |      - name: orders
      |        expression:
      |          sql: COUNT(*)
      |      - name: average
      |        expression:
      |          sql: AVG(amount)
      |      - name: distinct_amounts
      |        expression:
      |          sql: COUNT(DISTINCT source.amount)
      |metrics:
      |  - name: model_revenue
      |    expression:
      |      sql: SUM(orders.amount)
      |""".stripMargin
}

private class SemanticServer(path: String)
  extends RESTCatalogServer(
    path,
    new BearTokenAuthProvider("test-token"),
    new ConfigResponse(Collections.singletonMap("prefix", "paimon"), Collections.emptyMap()),
    "ossie-tests") {
  private val definitions = new ConcurrentHashMap[String, SemanticViewDefinition]()
  @volatile var semanticStatus: Int = 200
  @volatile var denySource: Boolean = false
  @volatile var authorizedSemanticReads: Int = 0
  @volatile var semanticWrites: Int = 0

  override def initDispatcher(auth: AuthProvider): Dispatcher = {
    val delegate = super.initDispatcher(auth)
    new Dispatcher {
      override def dispatch(request: RecordedRequest): MockResponse = {
        val route = request.getRequestUrl.encodedPath()
        def response(status: Int, body: String): MockResponse = new MockResponse()
          .setResponseCode(status)
          .setHeader("Content-Type", "application/json")
          .setBody(body)
        if (route.contains("/semantic-views/")) {
          val name = request.getRequestUrl
            .pathSegments()
            .get(request.getRequestUrl.pathSegments().size() - 1)
          if (request.getMethod == "POST") {
            semanticWrites += 1
            val value =
              RESTApi.fromJson(request.getBody.readUtf8(), classOf[UpsertSemanticViewRequest])
            definitions.put(name, value.getDefinition)
          } else {
            if (semanticStatus != 200) {
              return response(semanticStatus, "{\"message\":\"semantic access failed\"}")
            }
            if (request.getHeader("Authorization") == "Bearer test-token") {
              authorizedSemanticReads += 1
            }
          }
          val definition = definitions.get(name)
          if (definition == null) response(404, "{\"message\":\"missing semantic view\"}")
          else response(200, RESTApi.toJson(new GetSemanticViewResponse(name, definition)))
        } else if (denySource && route.endsWith("/tables/orders")) {
          response(403, "{\"message\":\"source access denied\"}")
        } else {
          delegate.dispatch(request)
        }
      }
    }
  }
}
