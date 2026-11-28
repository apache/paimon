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

import org.apache.paimon.spark.{PaimonHiveTestBase, SparkCatalog}
import org.apache.paimon.spark.catalog.SupportView
import org.apache.paimon.spark.catalyst.analysis.PaimonViewResolver
import org.apache.paimon.view.View

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.Identifier

import java.util.Collections

abstract class PaimonViewTestBase extends PaimonHiveTestBase {

  test("Paimon View: create and drop view") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        {
          sql(s"USE $catalogName")
          withDatabase("test_db") {
            sql("CREATE DATABASE test_db")
            sql("USE test_db")
            withTable("t") {
              withView("v1") {
                sql("CREATE TABLE t (id INT) USING paimon")
                sql("INSERT INTO t VALUES (1), (2)")

                sql("CREATE VIEW v1 AS SELECT * FROM t")
                checkAnswer(sql("SHOW VIEWS"), Seq(Row("test_db", "v1", false)))
                checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1), Row(2)))
                checkAnswer(
                  sql("SELECT * FROM v1 WHERE id >= (SELECT max(id) FROM v1)"),
                  Seq(Row(2)))

                // test drop view
                sql("DROP VIEW IF EXISTS v1")
                checkAnswer(sql("SHOW VIEWS"), Seq())
                sql("CREATE VIEW v1 AS SELECT * FROM t WHERE id > 1")
                checkAnswer(sql("SHOW VIEWS"), Seq(Row("test_db", "v1", false)))
                checkAnswer(sql("SELECT * FROM v1"), Seq(Row(2)))

                // test create or replace view
                intercept[Exception] {
                  sql("CREATE VIEW v1 AS SELECT * FROM t WHERE id < 2")
                }
                sql("CREATE OR REPLACE VIEW v1 AS SELECT * FROM t WHERE id < 2")
                checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1)))
              }
            }
          }
        }
    }
  }

  test("Paimon View: resolve unqualified names in the namespace of the view") {
    // spark_catalog does not support Paimon views, its views are Spark's own.
    Seq(paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        withDatabase("view_db1", "view_db2") {
          sql("CREATE DATABASE view_db1")
          sql("CREATE DATABASE view_db2")
          withTable("view_db1.orders", "view_db2.orders") {
            withView("view_db1.v", "view_db1.v_on_v", "view_db1.v_in", "view_db1.v_other") {
              sql("CREATE TABLE view_db1.orders (id INT) USING paimon")
              sql("INSERT INTO view_db1.orders VALUES (1)")
              // A different schema, so a view bound to this table would store another schema.
              sql("CREATE TABLE view_db2.orders (id INT, name STRING) USING paimon")
              sql("INSERT INTO view_db2.orders VALUES (2, 'b')")

              sql("USE view_db1")
              sql("CREATE VIEW v AS SELECT * FROM orders")
              sql("CREATE VIEW v_on_v AS SELECT * FROM v")
              sql("CREATE VIEW v_in AS SELECT * FROM orders WHERE id IN (SELECT id FROM orders)")
              sql("USE view_db2")
              // The body binds to the namespace of the view, also when it is created elsewhere.
              sql("CREATE VIEW view_db1.v_other AS SELECT * FROM orders")

              checkAnswer(sql("SELECT * FROM view_db1.v"), Seq(Row(1)))
              checkAnswer(sql("SELECT * FROM view_db1.v_on_v"), Seq(Row(1)))
              checkAnswer(sql("SELECT * FROM view_db1.v_in"), Seq(Row(1)))
              checkAnswer(sql("SELECT * FROM view_db1.v_other"), Seq(Row(1)))
              checkAnswer(
                sql("SELECT id FROM orders WHERE id NOT IN (SELECT id FROM view_db1.v)"),
                Seq(Row(2)))
              // The depth check follows the names the body resolves to.
              withSQLConf("spark.sql.view.maxNestedViewDepth" -> "1") {
                val error = intercept[IllegalArgumentException] {
                  sql("SELECT * FROM view_db1.v_on_v")
                }
                assert(error.getMessage.contains("spark.sql.view.maxNestedViewDepth (1)"))
              }
            }
          }
        }
    }
  }

  test("Paimon View: CTE in a subquery under the LEGACY CTE policy") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("view_db1", "view_db2") {
      sql("CREATE DATABASE view_db1")
      sql("CREATE DATABASE view_db2")
      withTable("view_db1.orders", "view_db2.orders") {
        withView("view_db1.v") {
          sql("CREATE TABLE view_db1.orders (id INT) USING paimon")
          sql("INSERT INTO view_db1.orders VALUES (1), (2)")
          sql("CREATE TABLE view_db2.orders (id INT) USING paimon")
          sql("INSERT INTO view_db2.orders VALUES (2), (3)")
          withSQLConf("spark.sql.legacy.ctePrecedencePolicy" -> "LEGACY") {
            sql("USE view_db1")
            sql("""
                  |CREATE VIEW v AS SELECT * FROM orders
                  |WHERE id IN (WITH c AS (SELECT id FROM orders) SELECT id FROM c)
                  |""".stripMargin)
            // Such a body cannot be qualified, so all its names bind in the current namespace,
            // never some here and some there.
            sql("USE view_db2")
            checkAnswer(sql("SELECT * FROM view_db1.v"), Seq(Row(2), Row(3)))
          }
          // The view records it, so it binds the same way under another CTE policy.
          checkAnswer(sql("SELECT * FROM view_db1.v"), Seq(Row(2), Row(3)))
        }
      }
    }
  }

  test("Paimon View: names resolved later in analysis bind in the current namespace") {
    // Paimon table-valued functions name their table with a string, and IDENTIFIER(...) is
    // resolved during analysis (before Spark 4.2 also with a literal). Such a body binds as before.
    val bodies =
      Seq("SELECT id FROM paimon_incremental_query('orders', 0, 1)") ++
        (if (gteqSpark3_5) Seq("SELECT id FROM IDENTIFIER('ord' || 'ers')") else Nil)
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("view_db1", "view_db2") {
      sql("CREATE DATABASE view_db1")
      sql("CREATE DATABASE view_db2")
      withTable("view_db1.orders", "view_db2.orders") {
        sql("CREATE TABLE view_db1.orders (id INT) USING paimon")
        sql("INSERT INTO view_db1.orders VALUES (1), (2)")
        sql("CREATE TABLE view_db2.orders (id INT) USING paimon")
        sql("INSERT INTO view_db2.orders VALUES (2), (3)")
        bodies.foreach {
          body =>
            withView("view_db1.v") {
              sql("USE view_db1")
              sql(s"CREATE VIEW v AS SELECT * FROM orders WHERE id IN ($body)")
              sql("USE view_db2")
              checkAnswer(sql("SELECT * FROM view_db1.v ORDER BY id"), Seq(Row(2), Row(3)))
            }
        }
      }
    }
  }

  test("Paimon View: views without the namespace option resolve in the current namespace") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("view_db1", "view_db2") {
      sql("CREATE DATABASE view_db1")
      sql("CREATE DATABASE view_db2")
      withTable("view_db1.orders", "view_db2.orders") {
        withView("view_db1.v") {
          sql("CREATE TABLE view_db1.orders (id INT) USING paimon")
          sql("INSERT INTO view_db1.orders VALUES (1)")
          sql("CREATE TABLE view_db2.orders (id INT) USING paimon")
          sql("INSERT INTO view_db2.orders VALUES (2)")
          // Simulate a view persisted by an older Paimon version, which has no such option.
          spark.sessionState.catalogManager.currentCatalog
            .asInstanceOf[SupportView]
            .createView(
              Identifier.of(Array("view_db1"), "v"),
              sql("SELECT * FROM view_db1.orders").schema,
              "SELECT * FROM orders",
              null,
              Collections.emptyMap[String, String](),
              false
            )
          sql("USE view_db2")
          checkAnswer(sql("SELECT * FROM view_db1.v"), Seq(Row(2)))

          // CREATE VIEW sets the option unless the statement sets it.
          sql("USE view_db1")
          sql("CREATE OR REPLACE VIEW v AS SELECT * FROM orders")
          sql("USE view_db2")
          checkAnswer(sql("SELECT * FROM view_db1.v"), Seq(Row(1)))
          sql("""
                |CREATE OR REPLACE VIEW view_db1.v
                |TBLPROPERTIES ('spark.view.resolve-in-view-namespace' = 'false')
                |AS SELECT * FROM orders
                |""".stripMargin)
          checkAnswer(sql("SELECT * FROM view_db1.v"), Seq(Row(2)))
        }
      }
    }
  }

  test("Paimon View: views resolving in their namespace do not read temporary views") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("view_db1", "view_db2") {
      sql("CREATE DATABASE view_db1")
      sql("CREATE DATABASE view_db2")
      withTable("view_db1.orders", "view_db2.orders") {
        withView(
          Seq("v", "v2", "v_false", "v_temp2", "v_temp3", "v_gtemp2").map("view_db1." + _): _*) {
          withTempView("orders", "v_temp", "v_temp2", "v_temp3") {
            sql("CREATE TABLE view_db1.orders (id INT) USING paimon")
            sql("INSERT INTO view_db1.orders VALUES (1)")
            sql("CREATE TABLE view_db2.orders (id INT) USING paimon")
            sql("INSERT INTO view_db2.orders VALUES (2)")
            sql("USE view_db1")
            sql("CREATE VIEW v AS SELECT id FROM orders")
            // On Spark 3.2 this is a Paimon view too, resolving in its namespace.
            sql("CREATE TEMPORARY VIEW v_temp3 AS SELECT id FROM orders")
            sql("SELECT 5 AS id").createOrReplaceTempView("orders")
            checkAnswer(sql("SELECT * FROM view_db1.v"), Row(1))
            // On Spark 3.2 Paimon turns CREATE TEMPORARY VIEW in its catalogs into a
            // persistent view.
            if (gteqSpark3_3) {
              sql("CREATE TEMPORARY VIEW v_temp AS SELECT * FROM view_db1.v")
              checkAnswer(sql("SELECT * FROM v_temp"), Row(1))
            }
            // On Spark 3.2 this is a Paimon view, which reads temporary views as before.
            sql("CREATE TEMPORARY VIEW v_temp2 AS SELECT id FROM orders")
            checkAnswer(sql("SELECT * FROM v_temp2"), Row(5))
            if (!gteqSpark3_3) {
              sql("CREATE GLOBAL TEMPORARY VIEW v_gtemp2 AS SELECT id FROM orders")
              checkAnswer(sql("SELECT * FROM view_db1.v_gtemp2"), Row(5))
              sql("DROP VIEW view_db1.v_gtemp2")
            }
            val error = intercept[UnsupportedOperationException] {
              sql("CREATE VIEW v2 AS SELECT id FROM orders")
            }
            assert(error.getMessage.contains("cannot read the temporary view orders"))
            // A view without the property reads temporary views, as before.
            sql("""
                  |CREATE VIEW v_false
                  |TBLPROPERTIES ('spark.view.resolve-in-view-namespace' = 'false')
                  |AS SELECT id FROM orders
                  |""".stripMargin)
            sql("USE view_db2")
            checkAnswer(sql("SELECT * FROM view_db1.v_false"), Row(5))
            sql("DROP VIEW orders")
            val tempView3 = if (gteqSpark3_3) "v_temp3" else "view_db1.v_temp3"
            checkAnswer(sql(s"SELECT * FROM $tempView3"), Row(1))
          }
        }
      }
    }
  }

  test("Paimon View: databases named like data sources resolve in the view catalog") {
    // A view of paimon_hive is read from the catalog paimon. parquet and orc name file sources,
    // jdbc names a source that cannot read files. Only paimon_hive has orc.
    val tables = Seq(
      paimonHiveCatalogName -> "parquet",
      paimonHiveCatalogName -> "jdbc",
      paimonHiveCatalogName -> "orc",
      "paimon" -> "parquet",
      "paimon" -> "jdbc")
    val ids = Map(paimonHiveCatalogName -> 1, "paimon" -> 2)
    val databases = tables.map { case (catalog, db) => s"$catalog.$db" }
    val view = s"$paimonHiveCatalogName.view_db1.v"
    sql(s"USE $paimonHiveCatalogName")
    withDatabase(databases :+ s"$paimonHiveCatalogName.view_db1": _*) {
      withTable(databases.map(db => s"$db.orders"): _*) {
        tables.foreach {
          case (catalog, db) =>
            sql(s"CREATE DATABASE $catalog.$db")
            sql(s"CREATE TABLE $catalog.$db.orders (id INT) USING paimon")
            sql(s"INSERT INTO $catalog.$db.orders VALUES (${ids(catalog)})")
        }
        sql("CREATE DATABASE view_db1")
        withView(view) {
          sql(s"""
                 |CREATE VIEW $view AS SELECT id FROM parquet.orders
                 |UNION ALL SELECT id FROM jdbc.orders UNION ALL SELECT id FROM orc.orders
                 |""".stripMargin)
          sql("USE paimon")
          checkAnswer(sql(s"SELECT * FROM $view"), Seq(Row(1), Row(1), Row(1)))
          // A path is still read as a file.
          withTempDir {
            dir =>
              val path = s"${dir.getCanonicalPath}/data"
              sql("SELECT 7 AS id").write.parquet(path)
              withView(s"$paimonHiveCatalogName.view_db1.v_file") {
                sql(s"CREATE VIEW $paimonHiveCatalogName.view_db1.v_file AS " +
                  s"SELECT id FROM parquet.`$path`")
                checkAnswer(sql(s"SELECT * FROM $paimonHiveCatalogName.view_db1.v_file"), Row(7))
              }
          }
          def assertNotFound(query: String): Unit = {
            val error = intercept[Exception](sql(query).collect())
            val message = error.getMessage.replace("`", "")
            assert(message.contains(s"$paimonHiveCatalogName.parquet.orders"), message)
          }
          withTempView("v_temp") {
            // Spark resolves the body of this temporary view in the catalog paimon. On Spark 3.2
            // Paimon turns CREATE TEMPORARY VIEW in its catalogs into a persistent view.
            if (gteqSpark3_3) {
              sql(s"CREATE TEMPORARY VIEW v_temp AS SELECT * FROM $view")
            }
            // Without the table in the view catalog, parquet.orders is still not read from the
            // catalog where Spark looks it up before reading a file.
            sql(s"DROP TABLE $paimonHiveCatalogName.parquet.orders")
            assertNotFound(s"SELECT * FROM $view")
            if (gteqSpark3_3) {
              sql(s"USE $paimonHiveCatalogName")
              assertNotFound("SELECT * FROM v_temp")
            }
          }
          sql(s"USE $paimonHiveCatalogName")
        }
      }
    }
  }

  test("Paimon View: reject recursive replacement") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withView("v1", "v2") {
        sql("CREATE VIEW v1 AS SELECT 1 AS id")
        val selfReference = intercept[IllegalArgumentException] {
          sql("CREATE OR REPLACE VIEW v1 AS SELECT * FROM v1")
        }
        assert(selfReference.getMessage.contains("Recursive Paimon view reference detected"))
        assert(
          selfReference.getMessage.contains("paimon_hive.test_db.v1 -> paimon_hive.test_db.v1"))
        checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1)))

        sql("CREATE VIEW v2 AS SELECT * FROM v1")
        val indirectReference = intercept[IllegalArgumentException] {
          sql("CREATE OR REPLACE VIEW v1 AS SELECT * FROM v2")
        }
        assert(
          indirectReference.getMessage.contains(
            "paimon_hive.test_db.v1 -> paimon_hive.test_db.v2 -> paimon_hive.test_db.v1"))
        checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1)))
      }
    }
  }

  test("Paimon View: reject persisted recursive views") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withView("v1", "v2") {
        val viewCatalog =
          spark.sessionState.catalogManager.currentCatalog.asInstanceOf[SupportView]
        val v1 = Identifier.of(Array("test_db"), "v1")
        val schema = sql("SELECT 1 AS id").schema
        sql("CREATE VIEW v1 AS SELECT 1 AS id")
        sql("CREATE VIEW v2 AS SELECT * FROM v1")
        Seq(
          "SELECT * FROM v1",
          "SELECT * FROM v2",
          "SELECT 1 AS id WHERE EXISTS (SELECT * FROM v1)",
          "SELECT (SELECT max(id) FROM v1) AS id"
        ).foreach {
          query =>
            // Simulate a recursive view persisted by an older Paimon version.
            viewCatalog.dropView(v1, true)
            viewCatalog.createView(
              v1,
              schema,
              query,
              null,
              Collections.emptyMap[String, String](),
              false)
            val error = intercept[IllegalArgumentException] {
              sql("SELECT * FROM v1")
            }
            assert(error.getMessage.contains("Recursive Paimon view reference detected"))
            assert(error.getMessage.contains("paimon_hive.test_db.v1"))
        }
        // An invalid legacy definition can still be replaced with a valid one.
        sql("CREATE OR REPLACE VIEW v1 AS SELECT 2 AS id")
        checkAnswer(sql("SELECT * FROM v2"), Seq(Row(2)))
      }
    }
  }

  test("Paimon View: reject recursive replacement in subqueries") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withTable("t") {
        withView("v1") {
          sql("CREATE TABLE t (id INT) USING paimon")
          sql("INSERT INTO t VALUES (1)")
          sql("CREATE VIEW v1 AS SELECT * FROM t")

          Seq(
            "SELECT * FROM t WHERE EXISTS (SELECT * FROM v1)",
            "SELECT * FROM t WHERE id IN (SELECT id FROM v1)",
            "SELECT (SELECT max(id) FROM v1) AS id FROM t",
            "WITH c AS (SELECT * FROM v1) SELECT * FROM c"
          ).foreach {
            query =>
              val error = intercept[IllegalArgumentException] {
                sql(s"CREATE OR REPLACE VIEW v1 AS $query")
              }
              assert(error.getMessage.contains("paimon_hive.test_db.v1 -> paimon_hive.test_db.v1"))
              checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1)))
          }
        }
      }
    }
  }

  test("Paimon View: reject recursive replacement with mixed case identifiers") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withView("v1") {
        sql("CREATE VIEW v1 AS SELECT 1 AS id")
        val error = intercept[IllegalArgumentException] {
          sql("CREATE OR REPLACE VIEW v1 AS SELECT * FROM TEST_DB.V1")
        }
        assert(error.getMessage.contains("paimon_hive.test_db.v1 -> paimon_hive.test_db.v1"))
        checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1)))
      }
    }
  }

  test("Paimon View: allow CTE shadowing and shared view dependencies") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withView("v1", "v2", "v3", "v4") {
        sql("CREATE VIEW v1 AS SELECT 1 AS id")
        sql("CREATE OR REPLACE VIEW v1 AS WITH v1 AS (SELECT 2 AS id) SELECT * FROM v1")
        checkAnswer(sql("SELECT * FROM v1"), Seq(Row(2)))
        sql("CREATE VIEW v2 AS SELECT * FROM v1")
        sql("CREATE VIEW v3 AS SELECT * FROM v1")
        sql("CREATE VIEW v4 AS SELECT * FROM v2 UNION ALL SELECT * FROM v3")
        checkAnswer(sql("SELECT * FROM v4"), Seq(Row(2), Row(2)))
      }
    }
  }

  test("Paimon View: reuse validation within a traversal but not across queries") {
    val catalogName = "paimon_counting_views"
    withSQLConf(
      s"spark.sql.catalog.$catalogName" -> classOf[CountingViewSparkCatalog].getName,
      s"spark.sql.catalog.$catalogName.metastore" -> "hive",
      s"spark.sql.catalog.$catalogName.uri" -> PaimonHiveTestBase.hiveUri,
      s"spark.sql.catalog.$catalogName.warehouse" -> tempHiveDBDir.getCanonicalPath
    ) {
      sql(s"USE $catalogName")
      withDatabase("test_db") {
        sql("CREATE DATABASE test_db")
        sql("USE test_db")
        val names = (0 until 8).map(i => s"v$i")
        withView(names: _*) {
          sql("CREATE VIEW v0 AS SELECT 1 AS id")
          names.indices.drop(1).foreach {
            i => sql(s"CREATE VIEW ${names(i)} AS SELECT * FROM ${names(i - 1)}")
          }
          val catalog = spark.sessionState.catalogManager.currentCatalog
            .asInstanceOf[CountingViewSparkCatalog]
          val resolver = PaimonViewResolver(spark)
          def query = spark.sessionState.sqlParser.parsePlan(s"SELECT * FROM ${names.last}")

          catalog.viewLoads = 0
          resolver(query)
          // A chain must not trigger another full dependency scan for every expanded view.
          assert(catalog.viewLoads <= names.size * 2, s"View loads: ${catalog.viewLoads}")

          // Reusing the rule for a new query must detect a newly persisted cycle.
          val first = Identifier.of(Array("test_db"), names.head)
          catalog.dropView(first, true)
          catalog.createView(
            first,
            sql("SELECT 1 AS id").schema,
            s"SELECT * FROM ${names.last}",
            null,
            Collections.emptyMap[String, String](),
            false)
          val error = intercept[IllegalArgumentException] {
            resolver(query)
          }
          assert(error.getMessage.contains("Recursive Paimon view reference detected"))
        }
      }
    }
  }

  test("Paimon View: enforce nested depth for shared dependencies") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withTable("t") {
        withView("v1", "v2", "v3", "v4") {
          sql("CREATE TABLE t (id INT) USING paimon")
          sql("INSERT INTO t VALUES (1)")
          sql("CREATE VIEW v1 AS SELECT * FROM t")
          sql("CREATE VIEW v2 AS SELECT * FROM v1")
          sql("CREATE VIEW v3 AS SELECT * FROM v2")
          sql("CREATE VIEW v4 AS SELECT * FROM v1 UNION ALL SELECT * FROM v3")
          withSQLConf("spark.sql.view.maxNestedViewDepth" -> "1") {
            checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1)))
          }
          withSQLConf("spark.sql.view.maxNestedViewDepth" -> "3") {
            checkAnswer(sql("SELECT * FROM v3"), Seq(Row(1)))
            val error = intercept[IllegalArgumentException] {
              sql("SELECT * FROM v4")
            }
            assert(error.getMessage.contains("spark.sql.view.maxNestedViewDepth (3)"))
          }
          withSQLConf("spark.sql.view.maxNestedViewDepth" -> "4") {
            checkAnswer(sql("SELECT * FROM v4"), Seq(Row(1), Row(1)))
          }
        }
      }
    }
  }

  test("Paimon View: show views") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        {
          sql(s"USE $catalogName")
          withDatabase("test_db") {
            sql("CREATE DATABASE test_db")
            sql("USE test_db")
            withTable("t") {
              withView("va", "vab", "vc") {
                sql("CREATE TABLE t (id INT) USING paimon")
                sql("CREATE VIEW va AS SELECT * FROM t")
                sql("CREATE VIEW vab AS SELECT * FROM t")
                sql("CREATE VIEW vc AS SELECT * FROM t")
                checkAnswer(
                  sql("SHOW VIEWS"),
                  Seq(
                    Row("test_db", "va", false),
                    Row("test_db", "vab", false),
                    Row("test_db", "vc", false)))

                // show views from database
                sql(s"USE $hiveDbName")
                checkAnswer(
                  sql("SHOW VIEWS FROM test_db"),
                  Seq(
                    Row("test_db", "va", false),
                    Row("test_db", "vab", false),
                    Row("test_db", "vc", false)))
                sql("USE test_db")

                checkAnswer(
                  sql("SHOW VIEWS LIKE 'va*'"),
                  Seq(Row("test_db", "va", false), Row("test_db", "vab", false)))
              }
            }
          }
        }
    }
  }

  test("Paimon View: show create view") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withTable("t") {
        withView("v") {
          sql("CREATE TABLE t (id INT, c STRING) USING paimon")
          sql("""
                |CREATE VIEW v
                |COMMENT 'test comment'
                |TBLPROPERTIES ('k1' = 'v1')
                |AS SELECT * FROM t
                |""".stripMargin)

          val s = sql("SHOW CREATE TABLE v").collectAsList().get(0).get(0).toString
          val r = """
                    |CREATE VIEW test_db.v \(
                    |  id,
                    |  c\)
                    |COMMENT 'test comment'
                    |TBLPROPERTIES \(
                    |  'k1' = 'v1',
                    |  'spark.view.resolve-in-view-namespace' = 'true',
                    |  'transient_lastDdlTime' = '\d+'\)
                    |AS
                    |SELECT \* FROM t
                    |""".stripMargin.replace("\n", "").r
          assert(r.findFirstIn(s.replace("\n", "")).isDefined)
        }
      }
    }
  }

  test("Paimon View: describe [extended] view") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withTable("t") {
        withView("v") {
          sql("CREATE TABLE t (id INT, c STRING) USING paimon")
          sql("""
                |CREATE VIEW v
                |COMMENT 'test comment'
                |TBLPROPERTIES ('k1' = 'v1')
                |AS SELECT * FROM t
                |""".stripMargin)

          checkAnswer(sql("DESC TABLE v"), Seq(Row("id", "INT", null), Row("c", "STRING", null)))

          val rows = sql("DESC TABLE EXTENDED v").collectAsList()
          assert(rows.get(3).toString().equals("[# Detailed View Information,,]"))
          assert(rows.get(4).toString().equals("[Name,test_db.v,]"))
          assert(rows.get(5).toString().equals("[Comment,test comment,]"))
          assert(rows.get(6).toString().equals("[View Text,SELECT * FROM t,]"))
          assert(rows.get(7).toString().equals("[View Query Output Columns,[id, c],]"))
          assert(rows.get(8).toString().contains("[View Properties,[k1=v1"))
        }
      }
    }
  }

  test("Paimon View: create temp view") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withTable("t") {
        withTempView("v1") {
          sql("CREATE TABLE t (id INT) USING paimon")
          sql("INSERT INTO t VALUES (1), (2)")

          sql("CREATE TEMPORARY VIEW v1 AS SELECT * FROM t")
          checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1), Row(2)))
          checkAnswer(sql("SELECT * FROM v1 WHERE id >= (SELECT max(id) FROM v1)"), Seq(Row(2)))

          sql("CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT * FROM t WHERE id < 2")
          checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1)))
        }
      }
    }
  }

  test("Paimon View: drop temp view") {
    sql(s"USE $paimonHiveCatalogName")
    withDatabase("test_db") {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
      withTable("t") {
        withTempView("v1") {
          sql("CREATE TABLE t (id INT) USING paimon")
          sql("INSERT INTO t VALUES (1), (2)")

          sql("CREATE TEMPORARY VIEW v1 AS SELECT * FROM t")
          sql("DROP VIEW v1")
        }
      }
    }
  }

  test("Paimon View: create view with CTE") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        withDatabase("test_db") {
          sql("CREATE DATABASE test_db")
          sql("USE test_db")
          withView("v1") {
            sql("""
                  |CREATE VIEW v1 AS
                  |    WITH t(a, b, c, d) AS (SELECT 1, 2, 3, 4)
                  |    SELECT * FROM t
                  |""".stripMargin)
            checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1, 2, 3, 4)))
          }
        }
    }
  }

  test("Paimon View: create view with ORDER BY ordinal") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        withDatabase("test_db") {
          sql("CREATE DATABASE test_db")
          sql("USE test_db")
          withTable("t") {
            withView("v_ord") {
              sql("CREATE TABLE t (id INT, name STRING) USING paimon")
              sql("INSERT INTO t VALUES (2, 'b'), (1, 'a')")
              sql("CREATE VIEW v_ord AS SELECT * FROM t ORDER BY 1")
              checkAnswer(sql("SELECT * FROM v_ord"), Seq(Row(1, "a"), Row(2, "b")))
            }
          }
        }
    }
  }

  test("Paimon View: create view on table with VARCHAR columns") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        withDatabase("test_db") {
          sql("CREATE DATABASE test_db")
          sql("USE test_db")
          withTable("t") {
            withView("v1") {
              sql("CREATE TABLE t (id INT, name VARCHAR(50), country STRING) USING paimon")
              sql("INSERT INTO t VALUES (1, 'alice', 'beijing'), (2, 'bob', 'shanghai')")
              sql("CREATE VIEW v1 AS SELECT * FROM t")
              checkAnswer(
                sql("SELECT * FROM v1"),
                Seq(Row(1, "alice", "beijing"), Row(2, "bob", "shanghai")))
            }
          }
        }
    }
  }

  test("Paimon View: create view on table with nested VARCHAR and CHAR columns") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        withDatabase("test_db") {
          sql("CREATE DATABASE test_db")
          sql("USE test_db")
          withTable("t") {
            withView("v1") {
              sql("""CREATE TABLE t (
                    |  id INT,
                    |  tags ARRAY<VARCHAR(100)>,
                    |  props MAP<STRING, VARCHAR(200)>
                    |) USING paimon""".stripMargin)
              sql("""INSERT INTO t VALUES
                    |  (1, ARRAY('a', 'b'), MAP('k1', 'v1')),
                    |  (2, ARRAY('c'), MAP('k2', 'v2'))""".stripMargin)
              sql("CREATE VIEW v1 AS SELECT * FROM t")
              checkAnswer(
                sql("SELECT * FROM v1"),
                Seq(Row(1, Seq("a", "b"), Map("k1" -> "v1")), Row(2, Seq("c"), Map("k2" -> "v2"))))
            }
          }
        }
    }
  }

  test("Paimon View: create view with column comments") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        withDatabase("test_db") {
          sql("CREATE DATABASE test_db")
          sql("USE test_db")
          withTable("t") {
            withView("v1") {
              sql("CREATE TABLE t (id INT, name STRING) USING paimon")
              sql("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

              // Create view with column comments
              sql("""
                    |CREATE VIEW v1 (
                    |  id COMMENT 'the user id',
                    |  name COMMENT 'the user name'
                    |) AS SELECT * FROM t
                    |""".stripMargin)

              // Verify view works
              checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1, "alice"), Row(2, "bob")))

              // Verify column comments via DESCRIBE
              val descRows = sql("DESC TABLE v1").collectAsList()
              assert(descRows.get(0).get(0).equals("id"))
              assert(descRows.get(0).get(2).equals("the user id"))
              assert(descRows.get(1).get(0).equals("name"))
              assert(descRows.get(1).get(2).equals("the user name"))

              // Verify column comments via SHOW CREATE TABLE
              val showCreateRows = sql("SHOW CREATE TABLE v1").collectAsList()
              val showCreateStr = showCreateRows.get(0).get(0).toString
              assert(showCreateStr.contains("COMMENT 'the user id'"))
              assert(showCreateStr.contains("COMMENT 'the user name'"))
            }
          }
        }
    }
  }

  test("Paimon View: create view with column aliases") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        withDatabase("test_db") {
          sql("CREATE DATABASE test_db")
          sql("USE test_db")
          withTable("t") {
            withView("v1") {
              sql("CREATE TABLE t (id INT, name STRING) USING paimon")
              sql("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

              // Create view with column aliases (without comments)
              sql("CREATE VIEW v1 (user_id, user_name) AS SELECT * FROM t")

              // Verify view works
              checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1, "alice"), Row(2, "bob")))

              // Verify column names via DESCRIBE
              val descRows = sql("DESC TABLE v1").collectAsList()
              assert(descRows.get(0).get(0).equals("user_id"))
              assert(descRows.get(1).get(0).equals("user_name"))
            }
          }
        }
    }
  }

  test("Paimon View: create view with column aliases and comments") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        withDatabase("test_db") {
          sql("CREATE DATABASE test_db")
          sql("USE test_db")
          withTable("t") {
            withView("v1") {
              sql("CREATE TABLE t (id INT, name STRING) USING paimon")
              sql("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

              // Create view with column aliases and comments
              sql("""
                    |CREATE VIEW v1 (
                    |  user_id COMMENT 'the user id',
                    |  user_name COMMENT 'the user name'
                    |) AS SELECT * FROM t
                    |""".stripMargin)

              // Verify view works
              checkAnswer(sql("SELECT * FROM v1"), Seq(Row(1, "alice"), Row(2, "bob")))

              // Verify column names and comments via DESCRIBE
              val descRows = sql("DESC TABLE v1").collectAsList()
              assert(descRows.get(0).get(0).equals("user_id"))
              assert(descRows.get(0).get(2).equals("the user id"))
              assert(descRows.get(1).get(0).equals("user_name"))
              assert(descRows.get(1).get(2).equals("the user name"))
            }
          }
        }
    }
  }
}

private[sql] class CountingViewSparkCatalog extends SparkCatalog {
  var viewLoads: Int = 0

  override def loadView(ident: Identifier): View = {
    viewLoads += 1
    super.loadView(ident)
  }
}
