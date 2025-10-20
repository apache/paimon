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

package org.apache.paimon.spark

import org.apache.paimon.catalog.Catalog.TableNotExistException

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

/**
 * Two-session reproduction of stale cache: session1 caches table, session2 drops & recreates, then
 * session1 commits with stale tableId and fails (without fix).
 */
class PaimonSparkTwoSessionCacheTest extends PaimonSparkTestWithRestCatalogBase {

  test("Two sessions: stale cache commit fails before fix") {
    val db = "sku"
    val tbl = "sku_detail_twosession"

    // s1 and s2 are independent SparkSessions sharing SparkContext but with separate Catalogs
    val s1 = spark
    val s2 = spark.newSession()

    // Ensure both sessions can see the database
    s1.sql(s"CREATE DATABASE IF NOT EXISTS paimon.$db")
    s2.sql(s"CREATE DATABASE IF NOT EXISTS paimon.$db")

    withTable(s"paimon.$db.$tbl") {
      s1.sql(s"""
        CREATE TABLE paimon.%s.%s (
          id INT,
          name STRING
        ) USING PAIMON
      """.format(db, tbl))

      // Session 1 caches table by reading/writing
      s1.sql(s"INSERT INTO paimon.%s.%s VALUES (1, 'a'), (2, 'b')".format(db, tbl))
      checkAnswer(
        s1.sql(s"SELECT * FROM paimon.%s.%s ORDER BY id".format(db, tbl)),
        Seq(Row(1, "a"), Row(2, "b")))

      // Session 2 drops and recreates the table (new tableId on server)
      s2.sql(s"DROP TABLE IF EXISTS paimon.%s.%s".format(db, tbl))
      s2.sql(s"""
        CREATE TABLE paimon.%s.%s (
          id INT,
          name STRING
        ) USING PAIMON
      """.format(db, tbl))
      s2.sql(s"INSERT INTO paimon.%s.%s VALUES (3, 'c')".format(db, tbl))

      // Session 1 attempts another write using stale cache (before fix this should fail)
      val thrown = intercept[Exception] {
        s1.sql(s"INSERT INTO paimon.%s.%s VALUES (4, 'd')".format(db, tbl))
      }
      // Expect a not-exist related failure
      assert(thrown.getCause.isInstanceOf[TableNotExistException])
      assert(thrown.getMessage.contains("Commit failed"))
    }
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // Keep caching enabled to exercise CachingCatalog
      .set("spark.sql.catalog.paimon.cache-enabled", "true")
      .set("spark.sql.catalog.paimon.cache-expire-after-access", "5m")
      .set("spark.sql.catalog.paimon.cache-expire-after-write", "10m")
      // Avoid plugin/codegen instability in UT while still exercising commitSnapshot
      .set("spark.sql.catalog.paimon.codegen-enabled", "false")
      .set("spark.sql.catalog.paimon.plugin-enabled", "false")
      .set("spark.sql.catalog.paimon.plugin-dir", warehouse)
  }
}
