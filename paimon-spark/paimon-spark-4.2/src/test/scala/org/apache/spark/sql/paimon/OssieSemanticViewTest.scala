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

import java.nio.file.Files
import java.util.Collections
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
