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

import org.apache.paimon.spark.catalyst.parser.extensions.PaimonSpark4SqlExtensionsParser
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/** Exercise the APIs that Spark 4.3 added. `Spark42CompatibilityTest` covers the rest. */
class Spark43CompatibilityTest extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", classOf[PaimonSparkSessionExtensions].getName)

  test("split statements through the Paimon parser") {
    val parser = spark.sessionState.sqlParser
    assert(parser.isInstanceOf[PaimonSpark4SqlExtensionsParser])
    val result = parser.splitStatements("SELECT ';'; SELECT 2;")
    assert(result.completeStatements.map(_.statement.trim) === Seq("SELECT ';'", "SELECT 2"))
    assert(result.partialStatement.isEmpty)
  }
}
