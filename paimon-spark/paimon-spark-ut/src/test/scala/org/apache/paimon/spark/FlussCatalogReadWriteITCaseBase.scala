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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.schema.SchemaChange
import org.apache.paimon.spark.catalog.SupportFluss
import org.apache.paimon.utils.FileIOUtils

import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.server.testutils.FlussClusterExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

import java.nio.file.{Files, Path}
import java.time.Duration

/** MiniCluster tests for reading and writing Fluss LakeStream tables through Paimon catalog. */
abstract class FlussCatalogReadWriteITCaseBase extends PaimonSparkTestBase {

  private val flussTestEnvironmentAvailable =
    scala.util.Properties.versionNumberString.startsWith("2.12.") && javaMajorVersion >= 11

  private val flussCatalogName = "fluss_catalog"
  private val flussDatabase = "fluss"
  private val lakeWarehouse: Path =
    Files.createTempDirectory("paimon-fluss-catalog-it").resolve("warehouse")

  private lazy val flussCluster: FlussClusterExtension =
    FlussClusterExtension.builder
      .setClusterConf(flussConfiguration)
      .setNumOfTabletServers(1)
      .build

  override protected val dbName0: String = flussDatabase

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    if (flussTestEnvironmentAvailable) {
      val bootstrapServers = flussCluster.getBootstrapServers
      val paimonExtensions = conf.get("spark.sql.extensions")

      conf
        .set(s"spark.sql.catalog.$flussCatalogName", "org.apache.fluss.spark.SparkCatalog")
        .set(s"spark.sql.catalog.$flussCatalogName.bootstrap.servers", bootstrapServers)
        .set(s"spark.sql.catalog.$flussCatalogName.datalake.format", "paimon")
        .set(s"spark.sql.catalog.$flussCatalogName.datalake.paimon.metastore", "filesystem")
        .set(
          s"spark.sql.catalog.$flussCatalogName.datalake.paimon.warehouse",
          lakeWarehouse.toString)
        .set("spark.sql.catalog.paimon.warehouse", lakeWarehouse.toString)
        .set("spark.sql.catalog.paimon.fluss.bootstrap.servers", bootstrapServers)
        .set("spark.sql.catalog.paimon.fluss.datalake.format", "paimon")
        .set("spark.sql.catalog.paimon.fluss.datalake.paimon.metastore", "filesystem")
        .set("spark.sql.catalog.paimon.fluss.datalake.paimon.warehouse", lakeWarehouse.toString)
        .set(
          "spark.sql.extensions",
          s"$paimonExtensions,org.apache.fluss.spark.FlussSparkSessionExtensions")
    } else {
      conf
    }
  }

  override protected def beforeAll(): Unit = {
    if (flussTestEnvironmentAvailable) {
      flussCluster.start()
    }
    try {
      super.beforeAll()
    } catch {
      case t: Throwable =>
        if (flussTestEnvironmentAvailable) {
          flussCluster.close()
        }
        throw t
    }
  }

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (flussTestEnvironmentAvailable) {
          flussCluster.close()
        }
      } finally {
        FileIOUtils.deleteDirectoryQuietly(lakeWarehouse.getParent.toFile)
      }
    }
  }

  test("write a Fluss log table and read its lake and real-time views") {
    assume(
      flussTestEnvironmentAvailable,
      "Fluss Spark integration tests require Scala 2.12 and Java 11 or later")
    withFlussTable("log_orders") {
      verifyLakeStreamMarker("log_orders")

      sql("INSERT INTO paimon.fluss.log_orders VALUES (1, 'a'), (2, 'b')")
      checkAnswer(sql("SELECT * FROM paimon.fluss.log_orders"), Nil)
      checkAnswer(
        sql("SELECT * FROM paimon.fluss.`log_orders$rt`"),
        Row(1, "a") :: Row(2, "b") :: Nil)

      sql("INSERT INTO fluss_catalog.fluss.log_orders VALUES (3, 'c')")
      checkAnswer(
        sql("SELECT * FROM paimon.fluss.`log_orders$rt`"),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  test("write a Fluss primary-key table and read its lake and real-time views") {
    assume(
      flussTestEnvironmentAvailable,
      "Fluss Spark integration tests require Scala 2.12 and Java 11 or later")
    withFlussTable("pk_orders", primaryKey = true) {
      verifyLakeStreamMarker("pk_orders")

      sql("INSERT INTO paimon.fluss.pk_orders VALUES (1, 'a'), (2, 'b')")
      checkAnswer(sql("SELECT * FROM paimon.fluss.pk_orders"), Nil)
      checkAnswer(
        sql("SELECT * FROM paimon.fluss.`pk_orders$rt`"),
        Row(1, "a") :: Row(2, "b") :: Nil)

      sql("INSERT INTO fluss_catalog.fluss.pk_orders VALUES (1, 'a2'), (3, 'c')")
      checkAnswer(
        sql("SELECT * FROM paimon.fluss.`pk_orders$rt`"),
        Row(1, "a2") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  private def withFlussTable(tableName: String, primaryKey: Boolean = false)(f: => Unit): Unit = {
    val primaryKeyProperty = if (primaryKey) ", 'primary.key' = 'id'" else ""
    val paimonPrimaryKeyProperties =
      if (primaryKey) {
        ", 'primary-key' = 'id', 'bucket' = '1', 'bucket-key' = 'id', " +
          "'changelog-producer' = 'input'"
      } else {
        ", 'bucket' = '-1'"
      }
    // Model the externally managed lifecycle: materialize matching Paimon lake metadata first and
    // publish the LakeStream marker only after the Fluss table exists.
    sql(s"""
           |CREATE TABLE paimon.$flussDatabase.$tableName (id INT, name STRING)
           |TBLPROPERTIES (
           |  'partition.legacy-name' = 'false'$paimonPrimaryKeyProperties)
           |""".stripMargin)
    try {
      sql(s"""
             |CREATE TABLE $flussCatalogName.$flussDatabase.$tableName (id INT, name STRING)
             |TBLPROPERTIES (
             |  'bucket.num' = '1',
             |  'table.datalake.enabled' = 'true',
             |  'table.datalake.format' = 'paimon',
             |  'table.datalake.freshness' = '60s'$primaryKeyProperty)
             |""".stripMargin)
      paimonCatalog.alterTable(
        Identifier.create(flussDatabase, tableName),
        java.util.Collections.singletonList(
          SchemaChange.setOption(SupportFluss.LAKESTREAM_ENABLED, "true")),
        false
      )
      try {
        f
      } finally {
        sql(s"DROP TABLE IF EXISTS $flussCatalogName.$flussDatabase.$tableName")
      }
    } finally {
      sql(s"DROP TABLE IF EXISTS paimon.$flussDatabase.$tableName")
    }
  }

  private def verifyLakeStreamMarker(tableName: String): Unit = {
    val table = loadTable(flussDatabase, tableName)
    assert(table.options().get(SupportFluss.LAKESTREAM_ENABLED) == "true")
  }

  private def flussConfiguration: Configuration = {
    val conf = new Configuration
    conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
    conf.setString("datalake.format", "paimon")
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    conf.setString("datalake.paimon.warehouse", lakeWarehouse.toString)
    conf.setString("server.data-disk.write-limit-ratio", "1.0")
    conf
  }

  private def javaMajorVersion: Int =
    System.getProperty("java.specification.version").split("\\.").last.toInt
}
