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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.rest.{RESTCatalogOptions, RESTReadVia}
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/** Tests for REST read-via propagation while resolving Paimon Views. */
class PaimonReadViaTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.paimon.${RESTCatalogOptions.READ_VIA_ENABLED.key}", "true")
  }

  test("read a table via a Paimon View") {
    withTable("source_table") {
      withView("read_view") {
        sql("CREATE TABLE source_table (id INT) USING paimon")
        sql("INSERT INTO source_table VALUES (1), (2)")
        sql("CREATE VIEW read_view AS SELECT * FROM source_table")
        restCatalogServer.clearReceivedTableIdentifiers()

        checkAnswer(sql("SELECT * FROM read_view"), Seq(Row(1), Row(2)))

        val expected = RESTReadVia.withReadVia(
          Identifier.create(dbName0, "source_table"),
          Identifier.create(dbName0, "read_view"))
        assert(restCatalogServer.getReceivedTableIdentifiers.asScala.contains(expected))
      }
    }
  }

  test("preserve the outermost Paimon View") {
    withTable("nested_source") {
      withView("inner_view", "outer_view") {
        sql("CREATE TABLE nested_source (id INT) USING paimon")
        sql("INSERT INTO nested_source VALUES (1), (2)")
        sql("CREATE VIEW inner_view AS SELECT * FROM nested_source")
        sql("CREATE VIEW outer_view AS SELECT * FROM inner_view")
        restCatalogServer.clearReceivedTableIdentifiers()

        checkAnswer(sql("SELECT * FROM outer_view"), Seq(Row(1), Row(2)))

        val source = Identifier.create(dbName0, "nested_source")
        val outer = Identifier.create(dbName0, "outer_view")
        val inner = Identifier.create(dbName0, "inner_view")
        val loaded = restCatalogServer.getReceivedTableIdentifiers.asScala
        assert(loaded.contains(RESTReadVia.withReadVia(source, outer)))
        assert(!loaded.contains(RESTReadVia.withReadVia(source, inner)))
      }
    }
  }

  test("mark relations in CTEs, joins, subqueries, and repeated references") {
    withTable("left_table", "right_table") {
      withView("complex_view") {
        sql("CREATE TABLE left_table (id INT) USING paimon")
        sql("CREATE TABLE right_table (id INT) USING paimon")
        sql("INSERT INTO left_table VALUES (1), (2)")
        sql("INSERT INTO right_table VALUES (1), (2)")
        sql("""
              |CREATE VIEW complex_view AS
              |WITH filtered AS (SELECT * FROM left_table WHERE id > 0)
              |SELECT f.id
              |FROM filtered f JOIN right_table r ON f.id = r.id
              |WHERE EXISTS (SELECT 1 FROM right_table r2 WHERE r2.id = f.id)
              |""".stripMargin)
        restCatalogServer.clearReceivedTableIdentifiers()

        checkAnswer(sql("SELECT * FROM complex_view"), Seq(Row(1), Row(2)))

        val root = Identifier.create(dbName0, "complex_view")
        val loaded = restCatalogServer.getReceivedTableIdentifiers.asScala
        assert(
          loaded.contains(RESTReadVia.withReadVia(Identifier.create(dbName0, "left_table"), root)))
        assert(
          loaded.contains(RESTReadVia.withReadVia(Identifier.create(dbName0, "right_table"), root)))
      }
    }
  }
}
