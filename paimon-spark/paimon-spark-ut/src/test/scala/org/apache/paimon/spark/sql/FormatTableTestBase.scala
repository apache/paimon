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

import org.apache.paimon.catalog.{DelegateCatalog, Identifier}
import org.apache.paimon.fs.Path
import org.apache.paimon.hive.HiveCatalog
import org.apache.paimon.spark.{PaimonFormatTableScan, PaimonHiveTestBase, PaimonInputPartition}
import org.apache.paimon.spark.PaimonHiveTestBase.hiveUri
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.source.Split
import org.apache.paimon.utils.CompressUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

abstract class FormatTableTestBase extends PaimonHiveTestBase with AdaptiveSparkPlanHelper {

  override protected def beforeEach(): Unit = {
    sql(s"USE $paimonHiveCatalogName")
    sql(s"USE $hiveDbName")
  }

  test("Format table: csv with field-delimiter") {
    withTable("t") {
      sql(s"CREATE TABLE t (f0 INT, f1 INT) USING CSV OPTIONS ('csv.field-delimiter' ';')")
      val table =
        paimonCatalog.getTable(Identifier.create(hiveDbName, "t")).asInstanceOf[FormatTable]
      val csvFile =
        new Path(table.location(), "part-00000-0a28422e-68ba-4713-8870-2fde2d36ed06-c000.csv")
      table.fileIO().writeFile(csvFile, "1;2\n3;4", false)
      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 2), Row(3, 4)))
    }
  }

  test("Format table: check partition sync") {
    val tableName = "t"
    withTable(tableName) {
      val hiveCatalog =
        paimonCatalog.asInstanceOf[DelegateCatalog].wrapped().asInstanceOf[HiveCatalog]
      sql(s"CREATE TABLE $tableName (f0 INT) USING CSV PARTITIONED BY (`ds` bigint)")
      sql(s"INSERT INTO $tableName VALUES (1, 2023)")
      var ds = 2023L
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(1, ds)))
      var partitions = hiveCatalog.listPartitionsFromHms(Identifier.create(hiveDbName, tableName))
      assert(partitions.size == 0)
      sql(s"DROP TABLE $tableName")
      sql(
        s"CREATE TABLE $tableName (f0 INT) USING CSV PARTITIONED BY (`ds` bigint, `hh` int) TBLPROPERTIES ('format-table.commit-hive-sync-url'='$hiveUri')")
      ds = 2024L
      val hh = 10
      sql(s"INSERT OVERWRITE $tableName PARTITION(ds=$ds, hh) VALUES (1, $hh)")
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(1, ds, hh)))
      partitions = hiveCatalog.listPartitionsFromHms(Identifier.create(hiveDbName, tableName))
      assert(partitions.get(0).getValues.get(0).equals(ds.toString))
      assert(partitions.get(0).getSd.getLocation.split("/").last.equals(s"hh=$hh"))
      sql(s"DROP TABLE $tableName")
      sql(s"CREATE TABLE $tableName (f0 INT) USING CSV PARTITIONED BY (`ds` bigint) " +
        s"TBLPROPERTIES ('format-table.commit-hive-sync-url'='$hiveUri', 'format-table.partition-path-only-value'='true')")
      ds = 2025L
      sql(s"INSERT INTO $tableName VALUES (1, $ds)")
      partitions = hiveCatalog.listPartitionsFromHms(Identifier.create(hiveDbName, tableName))
      assert(partitions.get(0).getSd.getLocation.split("/").last.equals(ds.toString))
    }
  }

  test("Format table: write partitioned table") {
    for (
      (format, compression) <- Seq(
        ("csv", "gzip"),
        ("orc", "zlib"),
        ("parquet", "zstd"),
        ("json", "none"))
    ) {
      withTable("t") {
        sql(
          s"CREATE TABLE t (id INT, p1 INT, p2 INT) USING $format PARTITIONED BY (p1, p2) TBLPROPERTIES ('file.compression'='$compression')")
        sql("INSERT INTO t VALUES (1, 2, 3)")

        // check show create table
        assert(
          sql("SHOW CREATE TABLE t").collectAsList().toString.contains("PARTITIONED BY (p1, p2)"))

        // check partition in file system
        val table =
          paimonCatalog.getTable(Identifier.create(hiveDbName, "t")).asInstanceOf[FormatTable]
        val dirs = table.fileIO().listStatus(new Path(table.location())).map(_.getPath.getName)
        assert(dirs.count(_.startsWith("p1=")) == 1)

        // check select
        checkAnswer(sql("SELECT * FROM t"), Row(1, 2, 3))
        checkAnswer(sql("SELECT id FROM t"), Row(1))
        checkAnswer(sql("SELECT p1 FROM t"), Row(2))
        checkAnswer(sql("SELECT p2 FROM t"), Row(3))
      }
    }
  }

  test("Format table: show partitions") {
    for (
      (format, compression) <- Seq(
        ("csv", "gzip"),
        ("orc", "zlib"),
        ("parquet", "zstd"),
        ("json", "none"))
    ) {
      withTable("t") {
        sql(
          s"CREATE TABLE t (id INT, p1 INT, p2 STRING) USING $format PARTITIONED BY (p1, p2) TBLPROPERTIES ('file.compression'='$compression')")
        sql("INSERT INTO t VALUES (1, 1, '1')")
        sql("INSERT INTO t VALUES (2, 1, '1')")
        sql("INSERT INTO t VALUES (3, 2, '1')")
        sql("INSERT INTO t VALUES (3, 2, '2')")

        checkAnswer(
          spark.sql("SHOW PARTITIONS T"),
          Seq(Row("p1=1/p2=1"), Row("p1=2/p2=1"), Row("p1=2/p2=2")))

        checkAnswer(spark.sql("SHOW PARTITIONS T PARTITION (p1=1)"), Seq(Row("p1=1/p2=1")))

        checkAnswer(spark.sql("SHOW PARTITIONS T PARTITION (p1=2, p2='2')"), Seq(Row("p1=2/p2=2")))
      }
    }
  }

  test("Format table: CTAS with partitioned table") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 (id INT, p1 INT, p2 INT) USING csv PARTITIONED BY (p1, p2)")
      sql("INSERT INTO t1 VALUES (1, 2, 3)")

      assertThrows[UnsupportedOperationException] {
        sql("""
              |CREATE TABLE t2
              |USING csv
              |PARTITIONED BY (p1, p2)
              |AS SELECT * FROM t1
              |""".stripMargin)
      }
    }
  }

  test("Format table: read compressed files") {
    for (format <- Seq("csv", "json")) {
      withTable("compress_t") {
        sql(s"CREATE TABLE compress_t (a INT, b INT, c INT) USING $format")
        sql("INSERT INTO compress_t VALUES (1, 2, 3)")
        val table =
          paimonCatalog
            .getTable(Identifier.create(hiveDbName, "compress_t"))
            .asInstanceOf[FormatTable]
        val fileIO = table.fileIO()
        val file = fileIO
          .listStatus(new Path(table.location()))
          .filter(file => !file.getPath.getName.startsWith("."))
          .head
          .getPath
          .toUri
          .getPath
        CompressUtils.gzipCompressFile(file, file + ".gz")
        fileIO.deleteQuietly(new Path(file))
        checkAnswer(sql("SELECT * FROM compress_t"), Row(1, 2, 3))
      }
    }
  }

  test("Format table: field delimiter in HMS") {
    withTable("t1") {
      sql("CREATE TABLE t1 (id INT, p1 INT, p2 INT) USING csv OPTIONS ('csv.field-delimiter' ';')")
      val row = sql("SHOW CREATE TABLE t1").collect()(0)
      assert(row.toString().contains("'csv.field-delimiter' = ';'"))
    }
  }

  test("Format table: broadcast join for small table") {
    withTable("t") {
      sql("CREATE TABLE t1 (f0 INT, f1 INT) USING CSV")
      sql("CREATE TABLE t2 (f0 INT, f2 INT) USING CSV")
      sql("INSERT INTO t1 VALUES (1, 1)")
      sql("INSERT INTO t2 VALUES (1, 1)")
      val df = sql("SELECT t1.f0, t1.f1, t2.f2 FROM t1, t2 WHERE t1.f0 = t2.f0")
      assert(df.queryExecution.executedPlan.toString().contains("BroadcastExchange"))
    }
  }

  test("Format table: format table and spark table props recognize") {
    val paimonFormatTblProps =
      """
        |'csv.field-delimiter'=';',
        |'csv.line-delimiter'='?',
        |'csv.quote-character'='%',
        |'csv.include-header'='true',
        |'csv.null-literal'='null',
        |'csv.mode'='permissive',
        |'format-table.file.compression'='gzip'
        |""".stripMargin

    val sparkTblProps =
      """
        |'sep'=';',
        |'lineSep'='?',
        |'quote'='%',
        |'header'='true',
        |'nullvalue'='null',
        |'mode'='permissive',
        |'compression'='gzip'
        |""".stripMargin

    val defaultProps = "'k'='v'"

    for (tblProps <- Seq(paimonFormatTblProps, sparkTblProps, defaultProps)) {
      withTable("t") {
        sql(s"CREATE TABLE t (id INT, v STRING) USING CSV TBLPROPERTIES ($tblProps)")
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id, id + 1 FROM range(2)")
        sql("INSERT INTO t VALUES (2, null)")

        for (impl <- Seq("engine", "paimon")) {
          withSparkSQLConf("spark.paimon.format-table.implementation" -> impl) {
            checkAnswer(
              sql("SELECT * FROM t ORDER BY id"),
              Seq(Row(0, "1"), Row(1, "2"), Row(2, null))
            )
          }
        }
      }
    }
  }

  test("Paimon format table: text format") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE t (v STRING) USING text
             |TBLPROPERTIES ('text.line-delimiter'='?')
             |""".stripMargin)
      sql("INSERT INTO t VALUES ('aaaa'), ('bbbb'), ('cccc'), (null), ('dddd')")

      for (impl <- Seq("paimon", "engine")) {
        withSparkSQLConf("spark.paimon.format-table.implementation" -> impl) {
          checkAnswer(sql("SELECT COUNT(*) FROM t"), Row(5))
          // Follow spark, write null as empty string and read as empty string too.
          checkAnswer(
            sql("SELECT * FROM t ORDER BY v"),
            Seq(Row(""), Row("aaaa"), Row("bbbb"), Row("cccc"), Row("dddd"))
          )
        }
      }
    }
  }

  test("Paimon format table: write text format with value with line-delimiter") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE t (v STRING) USING text
             |TBLPROPERTIES ('text.line-delimiter'='?')
             |""".stripMargin)
      sql("INSERT INTO t VALUES ('aa'), ('bb?cc')")

      for (impl <- Seq("paimon", "engine")) {
        withSparkSQLConf("spark.paimon.format-table.implementation" -> impl) {
          checkAnswer(sql("SELECT COUNT(*) FROM t"), Row(3))
          checkAnswer(
            sql("SELECT * FROM t ORDER BY v"),
            Seq(Row("aa"), Row("bb"), Row("cc"))
          )
        }
      }
    }
  }

  test("Paimon format table: runtime filter") {
    withTable("fact_table", "dim_table", "filter_table") {
      sql("""
            |CREATE TABLE fact_table (
            |  id INT,
            |  amount DOUBLE,
            |  category STRING,
            |  date_pt STRING
            |)
            |USING PARQUET
            |TBLPROPERTIES ('format-table.implementation'='paimon')
            |PARTITIONED BY (date_pt)
            |""".stripMargin)

      sql("""
            |CREATE TABLE dim_table (
            |  category STRING,
            |  category_name STRING,
            |  region STRING,
            |  pt STRING
            |)
            |USING PARQUET
            |TBLPROPERTIES ('format-table.implementation'='paimon')
            |PARTITIONED BY (pt)
            |""".stripMargin)

      sql("""
            |CREATE TABLE filter_table (
            |  region STRING,
            |  date_pt STRING
            |)
            |USING PARQUET
            |TBLPROPERTIES ('format-table.implementation'='paimon')
            |""".stripMargin)

      sql("""
            |INSERT INTO fact_table VALUES
            |(1, 100.0, 'A', '2023-01-01'),
            |(2, 200.0, 'B', '2023-01-02'),
            |(3, 150.0, 'A', '2023-01-15'),
            |(4, 250.0, 'C', '2023-02-01'),
            |(5, 300.0, 'A', '2023-02-15'),
            |(6, 180.0, 'B', '2024-01-01'),
            |(7, 220.0, 'A', '2024-01-15'),
            |(8, 400.0, 'C', '2024-02-01'),
            |(9, 350.0, 'B', '2024-02-15'),
            |(10, 500.0, 'A', '2025-03-01'),
            |(11, 450.0, 'C', '2025-03-15'),
            |(12, 600.0, 'B', '2025-04-01')
            |""".stripMargin)

      sql("""
            |INSERT INTO dim_table VALUES
            |('A', 'Category A', 'East', '2023-01'),
            |('B', 'Category B', 'West', '2023-01'),
            |('C', 'Category C', 'North', '2023-02'),
            |('A', 'Category A', 'East', '2024-01'),
            |('B', 'Category B', 'West', '2024-02'),
            |('C', 'Category C', 'North', '2024-02')
            |""".stripMargin)

      sql("""
            |INSERT INTO filter_table VALUES
            |('East', '2023-01-01'),
            |('East', '2023-01-15'),
            |('East', '2024-01-15')
            |""".stripMargin)

      val df = sql("""
                     |SELECT
                     |  f.id,
                     |  f.amount,
                     |  f.category,
                     |  d.category_name,
                     |  d.region,
                     |  f.date_pt
                     |FROM fact_table f
                     |JOIN dim_table d
                     |  ON f.category = d.category
                     |  AND SUBSTRING(f.date_pt, 1, 7) = d.pt
                     |JOIN filter_table ft
                     |  ON d.region = ft.region
                     |  AND f.date_pt = ft.date_pt
                     |WHERE d.region = 'East' AND f.date_pt < '2024-01-15'
                     |ORDER BY f.id
                     |""".stripMargin)

      checkAnswer(
        df,
        Seq(
          Row(1, 100.0, "A", "Category A", "East", "2023-01-01"),
          Row(3, 150.0, "A", "Category A", "East", "2023-01-15")
        )
      )

      val filteredSplits = collectFilteredInputSplits(df.queryExecution.executedPlan, "fact_table")
      assert(filteredSplits.size == 2)
    }
  }

  def collectFilteredInputSplits(plan: SparkPlan, tableName: String): Seq[Split] = {
    flatMap(plan) {
      case s: BatchScanExec =>
        s.scan match {
          case p: PaimonFormatTableScan if p.table.name() == tableName =>
            val filteredPartitionsField = s.getClass.getDeclaredField("filteredPartitions")
            filteredPartitionsField.setAccessible(true)
            val filteredPartitions = if (gteqSpark3_3) {
              filteredPartitionsField.get(s).asInstanceOf[Seq[Seq[InputPartition]]].flatten
            } else {
              filteredPartitionsField.get(s).asInstanceOf[Seq[InputPartition]]
            }
            filteredPartitions.flatMap { case p: PaimonInputPartition => p.splits }
          case _ => Nil
        }
      case _ => Nil
    }
  }
}
