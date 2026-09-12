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

package org.apache.spark.sql.paimon.shims

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction

class SparkVersionCompatTest extends PaimonSparkTestBase {

  private def catalogManager = spark.sessionState.catalogManager

  test("isBuiltinFunction recognizes a Spark builtin across versions") {
    val catalog = SparkVersionCompat.v1SessionCatalog(catalogManager)
    assert(SparkVersionCompat.isBuiltinFunction(catalog, "upper"))
  }

  test("isBuiltinFunction rejects a non-existent function") {
    val catalog = SparkVersionCompat.v1SessionCatalog(catalogManager)
    assert(!SparkVersionCompat.isBuiltinFunction(catalog, "paimon_no_such_fn"))
  }

  // Spark 4.2 turned `CatalogManager` from a class into an interface, which changes the invoke
  // opcode a direct call compiles to and makes a 4.2-built classfile unusable on 4.0/4.1. These
  // four accessors go through reflection to stay version-neutral; the tests assert they return
  // what a direct call would, so a wrong `Method` lookup cannot pass unnoticed.

  test("currentCatalog matches the session's configured catalog") {
    // `PaimonSparkTestBase.beforeEach` leaves the session on the `paimon` catalog.
    assert(SparkVersionCompat.currentCatalog(catalogManager).name() === "paimon")
  }

  test("catalog looks up a catalog by name") {
    val byName = SparkVersionCompat.catalog(catalogManager, "paimon")
    assert(byName === SparkVersionCompat.currentCatalog(catalogManager))
  }

  test("catalog propagates the original failure for an unknown name") {
    // `SparkUtils.catalogAndIdentifier` relies on catching this to fall back to the default
    // catalog, so the reflective layer must not wrap it in an InvocationTargetException.
    val e = intercept[Exception](SparkVersionCompat.catalog(catalogManager, "no_such_catalog"))
    assert(!e.isInstanceOf[java.lang.reflect.InvocationTargetException])
    assert(e.getMessage.contains("no_such_catalog"))
  }

  test("currentNamespace returns the session's namespace") {
    assert(SparkVersionCompat.currentNamespace(catalogManager).toSeq === Seq("test"))
  }

  test("v1SessionCatalog returns a usable SessionCatalog") {
    assert(SparkVersionCompat.v1SessionCatalog(catalogManager).databaseExists("default"))
  }

  test("ignoreNulls reads false for a plain UnresolvedFunction") {
    // `parseExpression` yields an unresolved node on every supported Spark version; building
    // `UnresolvedFunction` directly is not portable (the `Seq[String]`-first 3-arg apply does
    // not exist on Spark 3.5).
    val expr = spark.sessionState.sqlParser.parseExpression("upper('a')")
    val u = expr.collectFirst { case u: UnresolvedFunction => u }.get
    assert(!SparkVersionCompat.ignoreNulls(u))
  }

  test("ignoreNulls reads true when IGNORE NULLS is specified") {
    val expr = spark.sessionState.sqlParser
      .parseExpression("first(a) IGNORE NULLS")
    val u = expr.collectFirst { case u: UnresolvedFunction => u }.get
    assert(SparkVersionCompat.ignoreNulls(u))
  }

  // The `Option` shapes below only occur on Spark 4.2+, where `UnresolvedFunction.ignoreNulls`
  // returns `Option[Boolean]`. Testing the normalization directly keeps that branch covered on
  // every profile, instead of leaving it unexercised until the Spark baseline moves.

  test("toBoolean reads a boxed Boolean (Spark <= 4.1)") {
    assert(SparkVersionCompat.toBoolean(java.lang.Boolean.TRUE))
    assert(!SparkVersionCompat.toBoolean(java.lang.Boolean.FALSE))
  }

  test("toBoolean reads Some(true) (Spark 4.2+)") {
    assert(SparkVersionCompat.toBoolean(Some(true)))
  }

  test("toBoolean reads Some(false) (Spark 4.2+)") {
    assert(!SparkVersionCompat.toBoolean(Some(false)))
  }

  test("toBoolean treats None as not specified (Spark 4.2+)") {
    assert(!SparkVersionCompat.toBoolean(None))
  }

  test("toBoolean rejects an unrecognized shape instead of defaulting to false") {
    // Guards against a future Spark widening `ignoreNulls` again: silently reading such a value
    // as `false` would drop an IGNORE NULLS clause and return wrong results.
    val e = intercept[IllegalStateException](SparkVersionCompat.toBoolean(Some("yes")))
    assert(e.getMessage.contains("Unexpected UnresolvedFunction.ignoreNulls value"))
  }
}
