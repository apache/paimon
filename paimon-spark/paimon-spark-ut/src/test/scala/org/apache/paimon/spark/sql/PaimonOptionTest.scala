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

import org.apache.paimon.spark.{PaimonSparkTestBase, SparkConnectorOptions, SparkTable}
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTableFactory

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand
import org.apache.spark.sql.connector.catalog.{Identifier, TableCapability, TableCatalog}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.util.QueryExecutionListener
import org.junit.jupiter.api.Assertions

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

class PaimonOptionTest extends PaimonSparkTestBase {

  import testImplicits._

  test("Paimon Option: V2 write is enabled by default") {
    assert(SparkConnectorOptions.USE_V2_WRITE.defaultValue())
    SQLConf.withExistingConf(new SQLConf) {
      assert(OptionUtils.useV2Write())
    }
  }

  test("Paimon Option: V2 write respects explicit configuration") {
    for (enabled <- Seq(true, false)) {
      val conf = new SQLConf
      conf.setConfString("spark.paimon.write.use-v2-write", enabled.toString)
      SQLConf.withExistingConf(conf) {
        assert(OptionUtils.useV2Write() == enabled)
      }
    }
  }

  test("Paimon Option: default V2 write preserves table fallbacks") {
    val tableProperties = Seq(
      "'bucket' = '-1'" -> true,
      "'bucket-key' = 'id', 'bucket' = '2'" -> true,
      "'primary-key' = 'id', 'bucket' = '2'" -> true,
      "'primary-key' = 'id', 'bucket' = '-1'" -> false
    )
    for ((properties, supportsV2) <- tableProperties) {
      withTable("T") {
        sql(s"CREATE TABLE T (id INT) TBLPROPERTIES ($properties)")
        val table = SparkTable(loadTable("T"))
        assert(table.capabilities().contains(TableCapability.BATCH_WRITE) == supportsV2)
        assert(table.capabilities().contains(TableCapability.V1_BATCH_WRITE) == !supportsV2)
        sql("INSERT INTO T VALUES (1), (2)")
        checkAnswer(sql("SELECT * FROM T"), Seq(Row(1), Row(2)))
      }
    }
  }

  test("Paimon Option: V2 write can be disabled for supported tables") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "false") {
      sql("CREATE TABLE T (id INT) TBLPROPERTIES ('bucket-key' = 'id', 'bucket' = '2')")
      assert(SparkTable(loadTable("T")).capabilities().contains(TableCapability.V1_BATCH_WRITE))
      sql("INSERT INTO T VALUES (1), (2)")
      checkAnswer(sql("SELECT * FROM T"), Seq(Row(1), Row(2)))
    }
  }

  test("Paimon Option: V2 write follows merged table and session option precedence") {
    val globalKey = "spark.paimon.write.use-v2-write"
    val tableKey = s"spark.paimon.paimon.$dbName0.T.write.use-v2-write"
    for (tableEnabled <- Seq(false, true)) {
      withTable("T") {
        sql(s"""
               |CREATE TABLE T (id INT) TBLPROPERTIES (
               |  'bucket-key' = 'id', 'bucket' = '2',
               |  'write.use-v2-write' = '$tableEnabled')
               |""".stripMargin)
        val cases = Seq(
          Seq.empty[(String, String)] -> tableEnabled,
          Seq(globalKey -> "true") -> true,
          Seq(globalKey -> "false") -> false,
          Seq(globalKey -> "false", tableKey -> "true") -> true,
          Seq(globalKey -> "true", tableKey -> "false") -> false
        )
        for (((configs, expected), index) <- cases.zipWithIndex) {
          withSparkSQLConf(configs: _*) {
            val table = spark.sessionState.catalogManager.currentCatalog
              .asInstanceOf[TableCatalog]
              .loadTable(Identifier.of(Array(dbName0), "T"))
            assert(table.capabilities().contains(TableCapability.BATCH_WRITE) == expected)
            assert(table.capabilities().contains(TableCapability.V1_BATCH_WRITE) == !expected)
            sql(s"INSERT INTO T VALUES ($index)")
          }
        }
        checkAnswer(sql("SELECT * FROM T"), (0 until cases.size).map(Row(_)))
        assert(loadTable("T").options().get("write.use-v2-write") == tableEnabled.toString)
      }
    }
  }

  test("Paimon Option: table V2 write option cannot enable unsupported bucket modes") {
    sql("""
          |CREATE TABLE T (id INT) TBLPROPERTIES (
          |  'primary-key' = 'id', 'bucket' = '-1', 'write.use-v2-write' = 'true')
          |""".stripMargin)
    assert(SparkTable(loadTable("T")).capabilities().contains(TableCapability.V1_BATCH_WRITE))
    sql("INSERT INTO T VALUES (1), (2)")
    checkAnswer(sql("SELECT * FROM T"), Seq(Row(1), Row(2)))
  }

  test("Paimon Option: default V2 write distributes composite bucket keys and quoted columns") {
    withSparkSQLConf("spark.sql.shuffle.partitions" -> "3") {
      sql("""
            |CREATE TABLE T (`k.id` INT, k2 STRING, pt INT)
            |PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'k.id,k2,pt',
            |  'bucket-key' = 'k.id,k2',
            |  'bucket' = '4')
            |""".stripMargin)
      sql("""
            |INSERT INTO T SELECT
            |CAST(id % 12 AS INT), CAST(id % 3 AS STRING), CAST(id % 2 AS INT)
            |FROM range(0, 120, 1, 4)
            |""".stripMargin)
      checkAnswer(sql("SELECT * FROM T"), (0 until 12).map(i => Row(i, (i % 3).toString, i % 2)))
    }
  }

  test("Paimon Option: path-based writes fall back to V1 without a function catalog") {
    sql("CREATE TABLE T (id INT) TBLPROPERTIES ('bucket-key' = 'id', 'bucket' = '2')")
    Seq(1, 2)
      .toDF("id")
      .write
      .format("paimon")
      .mode("append")
      .save(loadTable("T").location().toString)
    checkAnswer(sql("SELECT * FROM T"), Seq(Row(1), Row(2)))
  }

  test("Paimon Option: DataSource catalog writes bind the catalog for V2 distribution") {
    for (adaptive <- Seq(true, false)) {
      withSparkSQLConf(
        "spark.sql.adaptive.enabled" -> adaptive.toString,
        "spark.sql.shuffle.partitions" -> "3",
        "spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
        withTable("T") {
          sql("""
                |CREATE TABLE T (id INT, pt INT) PARTITIONED BY (pt)
                |TBLPROPERTIES ('bucket-key' = 'id', 'bucket' = '1')
                |""".stripMargin)
          val location = loadTable("T").location().toString
          val input = spark
            .range(0, 40, 1, 4)
            .selectExpr("CAST(id AS INT) AS id", "CAST(id % 2 AS INT) AS pt")
          val writes = captureV2Writes {
            input.write
              .format("paimon")
              .option("catalog", "paimon")
              .option("database", dbName0)
              .option("table", "T")
              .mode("append")
              .save(location)
          }
          assert(writes.size == 1)
          val relation = writes.head.table.asInstanceOf[DataSourceV2Relation]
          assert(relation.catalog.exists(_.name() == "paimon"))
          assert(relation.identifier.contains(Identifier.of(Array(dbName0), "T")))
          assert(relation.table.capabilities().contains(TableCapability.BATCH_WRITE))
          checkAnswer(sql("SELECT * FROM T"), (0 until 40).map(i => Row(i, i % 2)))
          checkAnswer(sql("SELECT COUNT(*) FROM `T$files`"), Seq(Row(2L)))

          // A path overwrite still replaces the whole table, including partitions absent in input.
          val overwrites = captureV2Writes {
            Seq((100, 0))
              .toDF("id", "pt")
              .write
              .format("paimon")
              .option("catalog", "paimon")
              .option("database", dbName0)
              .option("table", "T")
              .mode("overwrite")
              .save(location)
          }
          assert(overwrites.size == 1)
          checkAnswer(sql("SELECT * FROM T"), Seq(Row(100, 0)))
        }
      }
    }
  }

  test("Paimon Option: DataSource write option overrides the session V2 write option") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "false") {
      sql("CREATE TABLE T (id INT) TBLPROPERTIES ('bucket-key' = 'id', 'bucket' = '2')")
      for (enabled <- Seq(false, true)) {
        val writes = captureV2Writes {
          Seq(1, 2)
            .toDF("id")
            .write
            .format("paimon")
            .option("catalog", "paimon")
            // Infer the database and table from the path when only the catalog is specified.
            .option("write.use-v2-write", enabled.toString)
            .mode("append")
            .save(loadTable("T").location().toString)
        }
        assert(writes.nonEmpty == enabled)
      }
      checkAnswer(sql("SELECT * FROM T"), Seq(Row(1), Row(2), Row(1), Row(2)))
    }
  }

  private def captureV2Writes(f: => Unit): Seq[V2WriteCommand] = {
    val writes = new ConcurrentLinkedQueue[V2WriteCommand]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        qe.analyzed match {
          case write: V2WriteCommand => writes.add(write)
          case _ =>
        }
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    Utils.waitUntilEventEmpty(spark)
    spark.listenerManager.register(listener)
    try {
      f
      Utils.waitUntilEventEmpty(spark)
      writes.asScala.toSeq
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("Paimon Option: create table with sql conf") {
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "2") {
      sql("CREATE TABLE T (id INT)")
      val table = loadTable("T")
      // check options in schema file directly
      val fileStoreTable = FileStoreTableFactory.create(table.fileIO(), table.location())
      Assertions.assertNull(fileStoreTable.options().get("scan.snapshot-id"))
    }
  }

  test("Paimon Option: create table by dataframe with sql conf") {
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "2") {
      Seq((1L, "x1"), (2L, "x2"))
        .toDF("a", "b")
        .write
        .format("paimon")
        .mode("append")
        .saveAsTable("T")
      val table = loadTable("T")
      // check options in schema file directly
      val fileStoreTable = FileStoreTableFactory.create(table.fileIO(), table.location())
      Assertions.assertNull(fileStoreTable.options().get("scan.snapshot-id"))
    }
  }

  test("Paimon Option: create table with Iceberg compatibility options via DataFrame writer") {
    Seq((1L, "x1"), (2L, "x2"))
      .toDF("a", "b")
      .write
      .format("paimon")
      .option("primary-key", "a")
      .option("bucket", "-1")
      .option("metadata.iceberg.database", "db_t")
      .option("metadata.iceberg.table", "t_ib")
      .option("metadata.iceberg.storage", "hadoop-catalog")
      .option("metadata.iceberg.storage-location", "table-location")
      .option("metadata.iceberg.manifest-legacy-version", "true")
      .option("metadata.iceberg.manifest-compression", "snappy")
      .option("metadata.iceberg.previous-versions-max", "5")
      .option("metadata.iceberg.uri", "")
      .saveAsTable("T_IB")

    val table = loadTable("T_IB")

    // Verify primary key is also stored (existing functionality still works)
    Assertions.assertEquals(1, table.primaryKeys().size())
    Assertions.assertEquals("a", table.primaryKeys().get(0))

    // Verify bucket configuration
    Assertions.assertEquals("-1", table.options().get("bucket"))

    // Verify Iceberg compatibility options are stored permanently
    Assertions.assertEquals("db_t", table.options().get("metadata.iceberg.database"))
    Assertions.assertEquals("t_ib", table.options().get("metadata.iceberg.table"))
    Assertions.assertEquals("hadoop-catalog", table.options().get("metadata.iceberg.storage"))
    Assertions.assertEquals(
      "table-location",
      table.options().get("metadata.iceberg.storage-location"))
    Assertions.assertEquals("true", table.options().get("metadata.iceberg.manifest-legacy-version"))
    Assertions.assertEquals("snappy", table.options().get("metadata.iceberg.manifest-compression"))
    Assertions.assertEquals("5", table.options().get("metadata.iceberg.previous-versions-max"))
    Assertions.assertEquals("", table.options().get("metadata.iceberg.uri"))
  }

  test("Paimon Option: query table with sql conf") {
    sql("CREATE TABLE T (id INT)")
    sql("INSERT INTO T VALUES 1")
    sql("INSERT INTO T VALUES 2")
    checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1) :: Row(2) :: Nil)
    val table = loadTable("T")

    // query with mutable option
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1))
      checkAnswer(spark.read.format("paimon").load(table.location().toString), Row(1))
    }

    // query with immutable option
    withSparkSQLConf("spark.paimon.bucket" -> "1") {
      assertThrows[UnsupportedOperationException] {
        sql("SELECT * FROM T ORDER BY id")
      }
      assertThrows[UnsupportedOperationException] {
        spark.read.format("paimon").load(table.location().toString)
      }
    }
  }

  test("Paimon Option: global deletion-vector merge-on-read supports mixed tables") {
    withTable("non_dv", "dv") {
      sql("CREATE TABLE non_dv (id INT, v STRING)")
      sql("""
            |CREATE TABLE dv (id INT, v STRING)
            |TBLPROPERTIES (
            |  'primary-key' = 'id',
            |  'bucket' = '1',
            |  'deletion-vectors.enabled' = 'true',
            |  'write-only' = 'true'
            |)
            |""".stripMargin)

      sql("INSERT INTO non_dv VALUES (1, 'append')")
      sql("INSERT INTO dv VALUES (2, 'dv')")

      checkAnswer(sql("SELECT * FROM dv"), Nil)
      withSparkSQLConf("spark.paimon.deletion-vectors.merge-on-read" -> "true") {
        checkAnswer(
          sql("""
                |SELECT * FROM non_dv
                |UNION ALL
                |SELECT * FROM dv
                |ORDER BY id
                |""".stripMargin),
          Row(1, "append") :: Row(2, "dv") :: Nil
        )
      }

      checkAnswer(
        spark.read
          .format("paimon")
          .option("deletion-vectors.merge-on-read", "true")
          .table("non_dv"),
        Row(1, "append"))
    }
  }

  test("Paimon Table Options: query one table with sql conf and table options") {
    sql("CREATE TABLE T (id INT)")
    sql("INSERT INTO T VALUES 1")
    sql("INSERT INTO T VALUES 2")
    checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1) :: Row(2) :: Nil)
    val table = loadTable("T")

    // query with global options
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1))
      checkAnswer(spark.read.format("paimon").load(table.location().toString), Row(1))
    }

    // query with table options
    withSparkSQLConf("spark.paimon.*.*.T.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1))
      checkAnswer(spark.read.format("paimon").table("T"), Row(1))
    }

    // query with both global and table options
    withSparkSQLConf(
      "spark.paimon.scan.snapshot-id" -> "1",
      "spark.paimon.*.*.T.scan.snapshot-id" -> "2") {
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1) :: Row(2) :: Nil)
      checkAnswer(spark.read.format("paimon").table("T"), Row(1) :: Row(2) :: Nil)
    }
  }

  test("Paimon Table Options: query multiple tables with sql conf and table options") {
    sql("CREATE TABLE T1 (id INT)")
    sql("INSERT INTO T1 VALUES 1")
    sql("INSERT INTO T1 VALUES 2")

    sql("CREATE TABLE T2 (id INT)")
    sql("INSERT INTO T2 VALUES 1")
    sql("INSERT INTO T2 VALUES 2")
    checkAnswer(
      sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"),
      Row(1, 1) :: Row(2, 2) :: Nil)
    val table1 = loadTable("T1")
    val table2 = loadTable("T1")

    // query with global options
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"), Row(1, 1))
      checkAnswer(
        spark.read
          .format("paimon")
          .table("T1")
          .join(spark.read.format("paimon").table("T2"), "id"),
        Row(1)
      )
    }

    // query with table options
    withSparkSQLConf("spark.paimon.*.*.*.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"), Row(1, 1))
      checkAnswer(
        spark.read
          .format("paimon")
          .table("T1")
          .join(spark.read.format("paimon").table("T2"), "id"),
        Row(1)
      )
    }

    // query with both global and table options
    withSparkSQLConf(
      "spark.paimon.scan.snapshot-id" -> "1",
      "spark.paimon.*.*.*.scan.snapshot-id" -> "2") {
      checkAnswer(
        sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"),
        Row(1, 1) :: Row(2, 2) :: Nil)
      checkAnswer(
        spark.read
          .format("paimon")
          .table("T1")
          .join(spark.read.format("paimon").table("T2"), "id"),
        Row(1) :: Row(2) :: Nil
      )
    }

    withSparkSQLConf(
      "spark.paimon.*.*.T1.scan.snapshot-id" -> "1",
      "spark.paimon.*.*.T2.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"), Row(1, 1))
      checkAnswer(
        spark.read
          .format("paimon")
          .table("T1")
          .join(spark.read.format("paimon").table("T2"), "id"),
        Row(1)
      )
    }

    withSparkSQLConf(
      "spark.paimon.*.*.T1.scan.snapshot-id" -> "1",
      "spark.paimon.*.*.T2.scan.snapshot-id" -> "2") {
      checkAnswer(sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"), Row(1, 1))
      checkAnswer(
        spark.read
          .format("paimon")
          .table("T1")
          .join(spark.read.format("paimon").table("T2"), "id"),
        Row(1)
      )
    }

    withSparkSQLConf(
      "spark.paimon.*.*.T1.scan.snapshot-id" -> "2",
      "spark.paimon.*.*.T2.scan.snapshot-id" -> "2") {
      checkAnswer(
        sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"),
        Row(1, 1) :: Row(2, 2) :: Nil)
      checkAnswer(
        spark.read
          .format("paimon")
          .table("T1")
          .join(spark.read.format("paimon").table("T2"), "id"),
        Row(1) :: Row(2) :: Nil
      )
    }
  }
}

class PaimonConfigCheckTest extends SparkFunSuite {

  test("Paimon Option: required confs check") {
    for (checkConf <- Seq(true, false)) {
      val spark = SparkSession
        .builder()
        .master("local[2]")
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon.warehouse", Utils.createTempDir.getCanonicalPath)
        .config("spark.paimon.requiredSparkConfsCheck.enabled", checkConf.toString)
        .getOrCreate()
      try {
        if (checkConf) {
          assertThrows[RuntimeException](spark.sql("USE paimon"))
        } else {
          spark.sql("USE paimon")
        }
      } finally {
        spark.close()
      }
    }
  }

  test("Paimon Option: required confs check with temporary SQLConf") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .config("spark.sql.catalog.paimon.warehouse", Utils.createTempDir.getCanonicalPath)
      .config(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .config("spark.paimon.requiredSparkConfsCheck.enabled", "true")
      .getOrCreate()
    try {
      SQLConf.withExistingConf(new SQLConf) {
        OptionUtils.checkRequiredConfigurations(spark)
      }
    } finally {
      spark.close()
    }
  }

  test("Paimon Option: required confs switch with temporary SQLConf") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .config("spark.sql.catalog.paimon.warehouse", Utils.createTempDir.getCanonicalPath)
      .config("spark.paimon.requiredSparkConfsCheck.enabled", "false")
      .getOrCreate()
    try {
      SQLConf.withExistingConf(new SQLConf) {
        OptionUtils.checkRequiredConfigurations(spark)
      }
    } finally {
      spark.close()
    }
  }
}
