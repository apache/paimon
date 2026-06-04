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

import org.apache.paimon.spark.PaimonHiveTestBase

import org.apache.spark.sql.Row

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
