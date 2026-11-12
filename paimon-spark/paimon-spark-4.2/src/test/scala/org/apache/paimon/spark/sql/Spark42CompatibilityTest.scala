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

import org.apache.paimon.rest.RESTCatalogServer
import org.apache.paimon.rest.auth.BearTokenAuthProvider
import org.apache.paimon.rest.responses.ConfigResponse
import org.apache.paimon.spark.SparkCatalog
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

import java.nio.file.Files
import java.util.Collections

/** Exercise the APIs whose signatures changed in Spark 4.2 using real Paimon files. */
class Spark42CompatibilityTest extends QueryTest with SharedSparkSession {
  private val warehouse = Files.createTempDirectory("paimon-spark42-").toFile
  private lazy val server = {
    val s = new RESTCatalogServer(
      warehouse.toURI.toString,
      new BearTokenAuthProvider("test-token"),
      new ConfigResponse(Collections.singletonMap("prefix", "paimon"), Collections.emptyMap()),
      "spark42-tests"
    )
    s.start()
    s
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
    .set("spark.sql.catalog.paimon.metastore", "rest")
    .set("spark.sql.catalog.paimon.uri", server.getUrl)
    .set("spark.sql.catalog.paimon.warehouse", "spark42-tests")
    .set("spark.sql.catalog.paimon.token.provider", "bear")
    .set("spark.sql.catalog.paimon.token", "test-token")
    .set("spark.sql.defaultCatalog", "paimon")
    .set("spark.sql.extensions", classOf[PaimonSparkSessionExtensions].getName)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sql("CREATE DATABASE paimon.test")
    sql("USE paimon.test")
  }

  override protected def afterAll(): Unit = {
    try { super.afterAll() }
    finally {
      server.shutdown()
      org.apache.commons.io.FileUtils.deleteDirectory(warehouse)
    }
  }

  test("create, insert, CTAS and static and dynamic partition overwrite") {
    withTable("t", "copied") {
      sql(
        "CREATE TABLE t (id INT, label VARCHAR(20), pt STRING) PARTITIONED BY (pt) " +
          "TBLPROPERTIES ('bucket'='-1', 'file.format'='avro')")
      sql("INSERT INTO t VALUES (1, 'a', 'x'), (2, 'b', 'y')")
      sql("INSERT OVERWRITE t PARTITION (pt='x') SELECT 3, 'c'")
      checkAnswer(sql("SELECT * FROM t"), Seq(Row(3, "c", "x"), Row(2, "b", "y")))
      withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
        sql("INSERT OVERWRITE t SELECT 4, 'd', 'y'")
      }
      checkAnswer(sql("SELECT * FROM t"), Seq(Row(3, "c", "x"), Row(4, "d", "y")))
      sql("CREATE TABLE copied TBLPROPERTIES ('file.format'='avro') AS SELECT * FROM t")
      checkAnswer(sql("SELECT * FROM copied"), sql("SELECT * FROM t"))
    }
  }

  test("ordinary views, table description and partition description") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id INT, pt STRING) PARTITIONED BY (pt) " +
          "TBLPROPERTIES ('bucket'='-1', 'file.format'='avro')")
      sql("INSERT INTO t VALUES (1, 'x')")
      try {
        sql("CREATE VIEW v AS SELECT id FROM t")
        checkAnswer(sql("SELECT * FROM v"), Seq(Row(1)))
        assert(sql("DESCRIBE t").collect().exists(_.getString(0) == "id"))
        assert(
          sql("DESCRIBE EXTENDED t PARTITION (pt='x')")
            .collect()
            .exists(_.getString(0) == "# Detailed Partition Information"))
      } finally { sql("DROP VIEW IF EXISTS v") }
    }
  }

  test("SQL scalar functions, builtin lookup and unsupported table function rejection") {
    try {
      sql("CREATE FUNCTION add_one(x INT) RETURNS INT RETURN x + 1")
      checkAnswer(
        sql("SELECT add_one(4), SUM(abs(v)) FROM VALUES (-2), (3) AS t(v)"),
        Seq(Row(5, 5)))
      val unsupported = intercept[UnsupportedOperationException] {
        sql("CREATE FUNCTION rows_plus(x INT) RETURNS TABLE (v INT) RETURN SELECT x + 1")
      }
      assert(unsupported.getMessage.contains("SQL table functions"))
    } finally {
      sql("DROP FUNCTION IF EXISTS add_one")
      sql("DROP FUNCTION IF EXISTS rows_plus")
    }
  }

  private val modes: Seq[(String, String)] = Seq(
    "primary-key" -> "'bucket'='1', 'primary-key'='id'",
    "append" -> "'bucket'='-1'",
    "row-tracking" -> "'bucket'='-1', 'row-tracking.enabled'='true'",
    "deletion-vectors" -> "'bucket'='-1', 'deletion-vectors.enabled'='true'",
    "data-evolution" ->
      ("'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true', " +
        "'deletion-vectors.enabled'='true'")
  )

  for ((mode, properties) <- modes; v2 <- Seq(false, true)) {
    test(s"UPDATE, DELETE and MERGE on $mode tables with v2 writes $v2") {
      withSQLConf("spark.paimon.write.use-v2-write" -> v2.toString) {
        withTable("t") {
          sql(s"CREATE TABLE t (id INT, v INT) TBLPROPERTIES ($properties, 'file.format'='avro')")
          sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
          sql("UPDATE t SET v=11 WHERE id=1")
          checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 11), Row(2, 20), Row(3, 30)))
          sql("DELETE FROM t WHERE id=2")
          sql(
            "MERGE INTO t USING (SELECT 3 AS id, 33 AS v UNION ALL SELECT 4, 44) s " +
              "ON t.id=s.id WHEN MATCHED THEN UPDATE SET * " +
              "WHEN NOT MATCHED THEN INSERT *")
          checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 11), Row(3, 33), Row(4, 44)))
        }
      }
    }
  }

  test("native V2 rewrite handles subquery updates and insert-only merges") {
    withSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql(
          "CREATE TABLE t (id INT, v INT) " +
            "TBLPROPERTIES ('bucket'='-1', 'file.format'='avro')")
        sql("INSERT INTO t VALUES (1, 10), (2, 20)")
        sql("UPDATE t SET v=99 WHERE id IN (SELECT 1)")
        sql(
          "MERGE INTO t USING (SELECT 3 AS id, 30 AS v) s ON t.id=s.id " +
            "WHEN NOT MATCHED THEN INSERT *")
        checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 99), Row(2, 20), Row(3, 30)))
        sql("DELETE FROM t")
        checkAnswer(sql("SELECT COUNT(*) FROM t"), Seq(Row(0L)))
      }
    }
  }

  test("variant values and spatial scalar and array getters") {
    val point = "0101000000000000000000F03F0000000000000040"
    withTable("typed") {
      sql(
        "CREATE TABLE typed (v VARIANT, g GEOMETRY(3857), " +
          "a ARRAY<GEOMETRY(3857)>, gg GEOGRAPHY(4326)) " +
          "TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet')")
      sql(
        s"INSERT INTO typed SELECT parse_json('{\"id\":1}'), " +
          s"ST_GeomFromWKB(unhex('$point'), 3857), " +
          s"array(ST_GeomFromWKB(unhex('$point'), 3857)), ST_GeogFromWKB(unhex('$point'))")
      checkAnswer(
        sql(
          "SELECT variant_get(v, '$.id', 'int'), hex(ST_AsBinary(g)), " +
            "hex(ST_AsBinary(a[0])), hex(ST_AsBinary(gg)) FROM typed"),
        Seq(Row(1, point, point, point))
      )
    }
  }
}
