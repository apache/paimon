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
}
