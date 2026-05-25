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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class WriteMergeSchemaWithRestCatalogTest extends PaimonSparkTestWithRestCatalogBase {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.catalog.paimon.cache-enabled", "false")
  }

  test("Write merge schema with REST catalog: dataframe write") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      Seq((1, "1"), (2, "2"))
        .toDF("a", "b")
        .write
        .format("paimon")
        .mode("append")
        .saveAsTable("t")

      // new columns
      Seq((3, "3", 3))
        .toDF("a", "b", "c")
        .write
        .format("paimon")
        .mode("append")
        .option("write.merge-schema", "true")
        .saveAsTable("t")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(Row(1, "1", null), Row(2, "2", null), Row(3, "3", 3))
      )
    }
  }

  test("Write merge schema with REST catalog: sql write") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("CREATE TABLE t (a INT, b STRING)")
        sql("INSERT INTO t VALUES (1, '1'), (2, '2')")

        // new columns
        sql("INSERT INTO t BY NAME SELECT 3 AS a, '3' AS b, 3 AS c")
        checkAnswer(
          sql("SELECT * FROM t ORDER BY a"),
          Seq(Row(1, "1", null), Row(2, "2", null), Row(3, "3", 3))
        )
      }
    }
  }
}
