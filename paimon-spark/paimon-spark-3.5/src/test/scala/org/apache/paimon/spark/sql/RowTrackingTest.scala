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

import org.apache.paimon.spark.SparkCatalog
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{Row, SparkSession}

import java.io.File

class RowTrackingTest extends RowTrackingTestBase {}

class RowTrackingKryoTest extends RowTrackingTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }
}

class RowTrackingKryoLocalClusterTest extends SparkFunSuite {

  private var spark: SparkSession = _
  private var warehouse: File = _
  private var previousSparkHome: String = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val sparkHome = sparkHomeDir
    assume(sparkHome.exists(), s"Spark home does not exist: ${sparkHome.getAbsolutePath}")

    warehouse = createTempDir("paimon-row-tracking-kryo")
    previousSparkHome = System.getProperty("spark.test.home")
    System.setProperty("spark.test.home", sparkHome.getAbsolutePath)

    spark = SparkSession
      .builder()
      .master("local-cluster[2,1,1024]")
      .appName("RowTrackingKryoLocalClusterTest")
      .config("spark.ui.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
      .config("spark.sql.catalog.paimon.warehouse", warehouse.getCanonicalPath)
      .config("spark.sql.extensions", classOf[PaimonSparkSessionExtensions].getName)
      .getOrCreate()

    spark.sql("USE paimon")
    spark.sql("CREATE DATABASE IF NOT EXISTS test")
    spark.sql("USE test")
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    } finally {
      if (previousSparkHome == null) {
        System.clearProperty("spark.test.home")
      } else {
        System.setProperty("spark.test.home", previousSparkHome)
      }
      if (warehouse != null) {
        deleteRecursively(warehouse)
      }
      super.afterAll()
    }
  }

  test("Data Evolution: partial fields merge with kryo broadcast") {
    spark.sql("DROP TABLE IF EXISTS target")
    spark.sql("DROP TABLE IF EXISTS source")

    spark.sql("""
                |CREATE TABLE target (id INT, b INT, c INT, pt STRING)
                |TBLPROPERTIES (
                |  'row-tracking.enabled' = 'true',
                |  'data-evolution.enabled' = 'true')
                |PARTITIONED BY (pt)
                |""".stripMargin)
    spark.sql("INSERT INTO target VALUES (1, 10, 100, 'p0'), (2, 20, 200, 'p0')")

    spark.sql("CREATE TABLE source (id INT, b INT, pt STRING)")
    spark.sql("INSERT INTO source VALUES (1, 11, 'p0')")

    spark
      .sql("""
             |MERGE INTO target
             |USING source
             |ON target.id = source.id AND target.pt = source.pt
             |WHEN MATCHED THEN UPDATE SET target.b = source.b
             |""".stripMargin)
      .collect()

    assert(
      spark.sql("SELECT id, b, c, pt FROM target ORDER BY id").collect().toSeq ==
        Seq(Row(1, 11, 100, "p0"), Row(2, 20, 200, "p0")))
  }

  private def sparkHomeDir: File = {
    Option(System.getProperty("spark.test.home"))
      .orElse(sys.env.get("SPARK_HOME"))
      .map(new File(_))
      .getOrElse(new File(System.getProperty("user.home"), "Documents/program/spark"))
  }

  private def createTempDir(prefix: String): File = {
    val dir = File.createTempFile(prefix, "")
    assert(dir.delete())
    assert(dir.mkdir())
    dir
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        val children = file.listFiles()
        if (children != null) {
          children.foreach(deleteRecursively)
        }
      }
      file.delete()
    }
  }
}
