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

import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import scala.collection.JavaConverters._

/** Merging scalar subqueries over tables with query auth, which needs a REST catalog. */
abstract class MergeScalarSubqueriesQueryAuthTestBase extends PaimonSparkTestWithRestCatalogBase {

  /** Scans in the executed plan; a reused subquery is a leaf and is not counted. */
  private def scanCount(sqlText: String): Int = {
    var n = -1
    withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
      n = sql(sqlText).queryExecution.executedPlan.collectWithSubqueries {
        case s: BatchScanExec => s
      }.size
    }
    n
  }

  private val query = "SELECT (SELECT sum(a) FROM T), (SELECT sum(b) FROM T)"

  /** Whether the table lets Spark merge its scans that read different columns (`SCAN_MERGING`). */
  private def mergedBySpark(tableName: String): Boolean =
    spark.sessionState.catalogManager.currentCatalog
      .asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(Array(dbName0), tableName))
      .capabilities()
      .asScala
      .exists(_.name == "SCAN_MERGING")

  test("merge subqueries that read different columns of a table without query auth") {
    withTable("T") {
      sql("CREATE TABLE T (a INT, b INT)")
      sql("INSERT INTO T VALUES (1, 10), (2, 20)")
      checkAnswer(sql(query), Row(3L, 30L))
      assert(scanCount(query) == 1)
    }
  }

  test("do not merge subqueries that read different columns of a table with query auth") {
    withTable("T") {
      sql("CREATE TABLE T (a INT, b INT) TBLPROPERTIES ('query-auth.enabled' = 'true')")
      sql("INSERT INTO T VALUES (1, 10), (2, 20)")
      checkAnswer(sql(query), Row(3L, 30L))
      assert(scanCount(query) == 2)
    }
  }

  test("merge subqueries that read the same columns of a table with query auth") {
    withTable("T") {
      sql("CREATE TABLE T (a INT, b INT) TBLPROPERTIES ('query-auth.enabled' = 'true')")
      sql("INSERT INTO T VALUES (1, 10), (2, 20)")
      val sameColumns = "SELECT (SELECT sum(a) FROM T), (SELECT avg(a) FROM T)"
      checkAnswer(sql(sameColumns), Row(3L, 1.5))
      assert(scanCount(sameColumns) == 1)
    }
  }

  test("do not merge when the session enables query auth") {
    withTable("T") {
      sql("CREATE TABLE T (a INT, b INT)")
      sql("INSERT INTO T VALUES (1, 10), (2, 20)")
      withSparkSQLConf("spark.paimon.query-auth.enabled" -> "true") {
        checkAnswer(sql(query), Row(3L, 30L))
        assert(scanCount(query) == 2)
      }
    }
  }

  test("do not merge when a read option enables query auth") {
    withTable("T") {
      sql("CREATE TABLE T (a INT, b INT)")
      sql("INSERT INTO T VALUES (1, 10), (2, 20)")
      // When the table declares SCAN_MERGING, Spark decides by its capabilities, which do not see
      // read options.
      assume(!mergedBySpark("T"))
      withTempView("v") {
        spark.read.option("query-auth.enabled", "true").table("T").createOrReplaceTempView("v")
        val viewQuery = "SELECT (SELECT sum(a) FROM v), (SELECT sum(b) FROM v)"
        checkAnswer(sql(viewQuery), Row(3L, 30L))
        assert(scanCount(viewQuery) == 2)
      }
    }
  }
}
