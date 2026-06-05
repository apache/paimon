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

import org.junit.jupiter.api.Assertions

import scala.collection.JavaConverters._

class CatalogQualifiedCreateTableLikeTest extends PaimonSparkTestBase {

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
