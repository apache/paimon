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
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonHiveTestBase
import org.apache.paimon.table.FormatTable
import org.apache.paimon.utils.CompressUtils

import org.apache.spark.sql.Row

abstract class FormatTableTestBase extends PaimonHiveTestBase {

  override protected def beforeEach(): Unit = {
    sql(s"USE $paimonHiveCatalogName")
    sql(s"USE $hiveDbName")
  }

  test("Format table: csv with field-delimiter") {
    withTable("t") {
      sql(
        s"CREATE TABLE t (f0 INT, f1 INT) USING CSV OPTIONS ('csv.field-delimiter' ';') TBLPROPERTIES ('file.compression'='none')")
      val table =
        paimonCatalog.getTable(Identifier.create(hiveDbName, "t")).asInstanceOf[FormatTable]
      val csvFile =
        new Path(table.location(), "part-00000-0a28422e-68ba-4713-8870-2fde2d36ed06-c000.csv")
      table.fileIO().writeFile(csvFile, "1;2\n3;4", false)
      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 2), Row(3, 4)))
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
      sql(
        "CREATE TABLE t1 (id INT, p1 INT, p2 INT) USING csv PARTITIONED BY (p1, p2) TBLPROPERTIES ('file.compression'='none')")
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
        sql(
          s"CREATE TABLE compress_t (a INT, b INT, c INT) USING $format TBLPROPERTIES ('file.compression'='none')")
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
      sql(
        "CREATE TABLE t1 (id INT, p1 INT, p2 INT) USING csv OPTIONS ('csv.field-delimiter' ';') TBLPROPERTIES ('file.compression'='none')")
      val row = sql("SHOW CREATE TABLE t1").collect()(0)
      assert(row.toString().contains("'csv.field-delimiter' = ';'"))
    }
  }

  test("Format table: broadcast join for small table") {
    withTable("t") {
      sql("CREATE TABLE t1 (f0 INT, f1 INT) USING CSV TBLPROPERTIES ('file.compression'='none')")
      sql("CREATE TABLE t2 (f0 INT, f2 INT) USING CSV TBLPROPERTIES ('file.compression'='none')")
      sql("INSERT INTO t1 VALUES (1, 1)")
      sql("INSERT INTO t2 VALUES (1, 1)")
      val df = sql("SELECT t1.f0, t1.f1, t2.f2 FROM t1, t2 WHERE t1.f0 = t2.f0")
      assert(df.queryExecution.executedPlan.toString().contains("BroadcastExchange"))
    }
  }
}
