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

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.commands.PaimonCreateTableLikeCommand

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.junit.jupiter.api.Assertions

import scala.collection.JavaConverters._

class CatalogQualifiedCreateTableLikeTest extends PaimonSparkTestBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.external", classOf[InMemoryTableCatalog].getName)

  test("Create table like keeps the nested namespace of a source in another catalog") {
    assume(gteqSpark3_4)
    withTable(
      "external.outer.inner.source",
      "external.`outer.inner`.source",
      s"paimon.$dbName0.target_tbl") {
      sql("CREATE TABLE external.outer.inner.source (intended INT)")
      sql("CREATE TABLE external.`outer.inner`.source (wrong STRING)")

      sql(s"CREATE TABLE paimon.$dbName0.target_tbl LIKE external.outer.inner.source USING paimon")

      Assertions.assertEquals(
        Seq("intended"),
        spark.table(s"paimon.$dbName0.target_tbl").schema.fieldNames.toSeq)
    }
  }

  test("Create table like rejects a temporary view source for a Paimon target") {
    assume(gteqSpark3_4)
    withTable("shadowed", "target_tbl") {
      sql("CREATE TABLE shadowed (from_table STRING)")
      withTempView("shadowed") {
        sql("CREATE TEMPORARY VIEW shadowed AS SELECT 1 AS from_view")

        val e = intercept[UnsupportedOperationException] {
          sql("CREATE TABLE target_tbl LIKE shadowed")
        }

        Assertions.assertTrue(e.getMessage.contains("temporary view"), e.getMessage)
        Assertions.assertFalse(spark.catalog.tableExists(s"paimon.$dbName0.target_tbl"))
      }
    }
  }

  test("Create table like never puts a target of another catalog into the session catalog") {
    assume(gteqSpark3_4)
    withTable(
      "spark_catalog.default.src",
      "spark_catalog.default.t_external",
      "external.default.t_external") {
      sql("CREATE NAMESPACE IF NOT EXISTS external.default")
      sql("CREATE TABLE spark_catalog.default.src (a INT) USING parquet")

      val statement = "CREATE TABLE external.default.t_external LIKE spark_catalog.default.src"
      if (sparkVersion >= "4.2") {
        // Spark 4.2 parses the statement itself and creates the table in `external`.
        sql(statement)
        Assertions.assertEquals(
          Seq("a"),
          spark.table("external.default.t_external").schema.fieldNames.toSeq)
      } else {
        val e = intercept[UnsupportedOperationException](sql(statement))
        Assertions.assertTrue(e.getMessage.contains("session catalog"), e.getMessage)
      }

      Assertions.assertFalse(
        spark.sessionState.catalog.tableExists(TableIdentifier("t_external", Some("default"))))
    }
  }

  test("Create table like rejects a source of another catalog for a session catalog target") {
    // Spark 4.2 parses this statement itself and Paimon leaves this non-Paimon target to it, so
    // only earlier versions reach the check in `sessionCatalogCommand`.
    assume(gteqSpark3_4 && sparkVersion < "4.2")
    withTable("spark_catalog.default.src2", "spark_catalog.default.t2", "external.default.src2") {
      sql("CREATE NAMESPACE IF NOT EXISTS external.default")
      sql("CREATE TABLE external.default.src2 (from_external INT)")
      sql("CREATE TABLE spark_catalog.default.src2 (from_session STRING) USING parquet")

      val e = intercept[UnsupportedOperationException] {
        sql("CREATE TABLE spark_catalog.default.t2 LIKE external.default.src2")
      }

      Assertions.assertTrue(e.getMessage.contains("session catalog"), e.getMessage)
      Assertions.assertFalse(
        spark.sessionState.catalog.tableExists(TableIdentifier("t2", Some("default"))))
    }
  }

  test("Create table like keeps Spark's command for tables in the session catalog") {
    assume(gteqSpark3_4)
    withTable(
      "spark_catalog.default.src3",
      "spark_catalog.default.t3",
      "spark_catalog.default.t4") {
      sql("CREATE TABLE spark_catalog.default.src3 (a INT) USING parquet")

      sql("CREATE TABLE spark_catalog.default.t3 LIKE spark_catalog.default.src3")
      Assertions.assertEquals(
        Seq("a"),
        spark.table("spark_catalog.default.t3").schema.fieldNames.toSeq)

      withTempView("v") {
        sql("CREATE TEMPORARY VIEW v AS SELECT 1 AS from_view")
        sql("CREATE TABLE spark_catalog.default.t4 LIKE v USING parquet")
        Assertions.assertEquals(
          Seq("from_view"),
          spark.table("spark_catalog.default.t4").schema.fieldNames.toSeq)
      }
    }
  }

  test("Create table like with catalog-qualified identifiers") {
    assume(gteqSpark3_4)
    withTable("source_tbl", "target_tbl", "target_from_qualified_source", "qualified_target") {
      createSourceTable()

      sql(s"CREATE TABLE paimon.$dbName0.target_tbl LIKE paimon.$dbName0.source_tbl")
      assertCreatedLike("target_tbl")

      sql(s"CREATE TABLE target_from_qualified_source LIKE paimon.$dbName0.source_tbl")
      assertCreatedLike("target_from_qualified_source")

      sql(s"CREATE TABLE paimon.$dbName0.qualified_target LIKE source_tbl")
      assertCreatedLike("qualified_target")
    }
  }

  test("Create table like if not exists with catalog-qualified identifiers") {
    assume(gteqSpark3_4)
    withTable("source_tbl", "target_tbl") {
      createSourceTable()
      sql("""
            |CREATE TABLE target_tbl (
            |  id BIGINT,
            |  pt STRING
            |) COMMENT 'target comment'
            |PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'id,pt',
            |  'bucket' = '3'
            |)
            |""".stripMargin)

      val targetSchema = spark.table("target_tbl").schema
      val targetLocation = loadTable("target_tbl").location().toString

      sql(s"""
             |CREATE TABLE IF NOT EXISTS paimon.$dbName0.target_tbl
             |LIKE paimon.$dbName0.source_tbl
             |""".stripMargin)

      val target = loadTable("target_tbl")
      Assertions.assertEquals(targetSchema, spark.table("target_tbl").schema)
      Assertions.assertFalse(spark.table("target_tbl").schema.fieldNames.contains("name"))
      Assertions.assertEquals("target comment", target.comment().get())
      Assertions.assertEquals("3", target.options().get("bucket"))
      Assertions.assertEquals(targetLocation, target.location().toString)
    }
  }

  test("Create table like clauses with catalog-qualified identifiers") {
    assume(gteqSpark3_4)
    withTable("source_tbl", "target_tbl") {
      createSourceTable()

      sql(s"""
             |CREATE TABLE paimon.$dbName0.target_tbl
             |LIKE paimon.$dbName0.source_tbl
             |USING paimon
             |TBLPROPERTIES (
             |  'bucket' = '8',
             |  'target-file-size' = '256MB'
             |)
             |""".stripMargin)

      val source = loadTable("source_tbl")
      val target = loadTable("target_tbl")
      Assertions.assertEquals(spark.table("source_tbl").schema, spark.table("target_tbl").schema)
      Assertions.assertEquals("source comment", target.comment().get())
      Assertions.assertEquals(List("pt"), target.partitionKeys().asScala.toList)
      Assertions.assertEquals(List("id", "pt"), target.primaryKeys().asScala.toList)
      Assertions.assertEquals("8", target.options().get("bucket"))
      Assertions.assertEquals("256MB", target.options().get("target-file-size"))
      Assertions.assertNotEquals(source.location().toString, target.location().toString)
    }
  }

  test("Create table like stored as is unsupported with catalog-qualified identifiers") {
    assume(gteqSpark3_4)
    withTable("source_tbl", "target_tbl") {
      sql("CREATE TABLE source_tbl (id INT)")

      val error = intercept[Exception] {
        sql(s"""
               |CREATE TABLE paimon.$dbName0.target_tbl
               |LIKE paimon.$dbName0.source_tbl
               |STORED AS PARQUET
               |""".stripMargin)
      }.getMessage

      Assertions.assertTrue(
        error.contains("CREATE TABLE LIKE ... STORED AS is not supported for SparkCatalog."))
    }
  }

  test("Create table like parser accepts non-reserved and nested identifiers") {
    assume(gteqSpark3_4)

    val nonReservedIdentifierCommand =
      parseCreateTableLikeCommand("CREATE TABLE paimon.test.tag LIKE paimon.test.source_tbl")
    Assertions.assertEquals("tag", nonReservedIdentifierCommand.targetIdent.name())
    Assertions.assertEquals(Seq("test"), nonReservedIdentifierCommand.targetIdent.namespace().toSeq)

    val continueCommand =
      parseCreateTableLikeCommand("CREATE TABLE paimon.test.continue LIKE paimon.test.source_tbl")
    Assertions.assertEquals("continue", continueCommand.targetIdent.name())
    Assertions.assertEquals(Seq("test"), continueCommand.targetIdent.namespace().toSeq)

    val skipFileCommand =
      parseCreateTableLikeCommand("CREATE TABLE paimon.test.skip_file LIKE paimon.test.source_tbl")
    Assertions.assertEquals("skip_file", skipFileCommand.targetIdent.name())
    Assertions.assertEquals(Seq("test"), skipFileCommand.targetIdent.namespace().toSeq)

    val arrayCommand =
      parseCreateTableLikeCommand("CREATE TABLE paimon.test.array LIKE paimon.test.source_tbl")
    Assertions.assertEquals("array", arrayCommand.targetIdent.name())
    Assertions.assertEquals(Seq("test"), arrayCommand.targetIdent.namespace().toSeq)

    val nestedIdentifierCommand =
      parseCreateTableLikeCommand(
        "CREATE TABLE paimon.test.extra.target_tbl LIKE paimon.test.extra.source_tbl")
    Assertions.assertEquals("target_tbl", nestedIdentifierCommand.targetIdent.name())
    Assertions.assertEquals(
      Seq("test.extra"),
      nestedIdentifierCommand.targetIdent.namespace().toSeq)
    Assertions.assertEquals("source_tbl", nestedIdentifierCommand.sourceIdent.name())
    Assertions.assertEquals(
      Seq("test.extra"),
      nestedIdentifierCommand.sourceIdent.namespace().toSeq)
  }

  private def createSourceTable(): Unit = {
    sql("""
          |CREATE TABLE source_tbl (
          |  id BIGINT,
          |  name STRING COMMENT 'name column',
          |  pt STRING
          |) COMMENT 'source comment'
          |PARTITIONED BY (pt)
          |TBLPROPERTIES (
          |  'primary-key' = 'id,pt',
          |  'bucket' = '2',
          |  'target-file-size' = '64MB'
          |)
          |""".stripMargin)
  }

  private def assertCreatedLike(tableName: String): Unit = {
    val target = loadTable(tableName)

    Assertions.assertEquals(spark.table("source_tbl").schema, spark.table(tableName).schema)
    Assertions.assertEquals("source comment", target.comment().get())
    Assertions.assertEquals(List("pt"), target.partitionKeys().asScala.toList)
    Assertions.assertEquals(List("id", "pt"), target.primaryKeys().asScala.toList)
    Assertions.assertEquals("2", target.options().get("bucket"))
    Assertions.assertEquals("64MB", target.options().get("target-file-size"))
  }

  private def parseCreateTableLikeCommand(sqlText: String): PaimonCreateTableLikeCommand = {
    spark.sessionState.sqlParser.parsePlan(sqlText) match {
      case command: PaimonCreateTableLikeCommand => command
      case plan =>
        throw new AssertionError(
          s"Expected PaimonCreateTableLikeCommand, but got ${plan.nodeName}.")
    }
  }
}
