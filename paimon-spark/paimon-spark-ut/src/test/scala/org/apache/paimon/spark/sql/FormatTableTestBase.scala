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
import org.apache.paimon.utils.{CompressUtils, FileIOUtils, FileUtils}

import org.apache.spark.sql.Row

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.GZIPOutputStream

abstract class FormatTableTestBase extends PaimonHiveTestBase {

  override protected def beforeEach(): Unit = {
    sql(s"USE $paimonHiveCatalogName")
  }

  test("Format table: write partitioned table") {
    for (format <- Seq("csv", "orc", "parquet", "json")) {
      withTable("t") {
        sql(s"CREATE TABLE t (id INT, p1 INT, p2 INT) USING $format PARTITIONED BY (p1, p2)")
        sql("INSERT INTO t VALUES (1, 2, 3)")

        // check show create table
        assert(
          sql("SHOW CREATE TABLE t").collectAsList().toString.contains("PARTITIONED BY (p1, p2)"))

        // check partition in file system
        val table =
          paimonCatalog.getTable(Identifier.create("default", "t")).asInstanceOf[FormatTable]
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

  test("Format table: read compressed files") {
    for (format <- Seq("csv", "json")) {
      withTable("compress_t") {
        sql(s"CREATE TABLE compress_t (a INT, b INT, c INT) USING $format")
        sql("INSERT INTO compress_t VALUES (1, 2, 3)")
        val table =
          paimonCatalog
            .getTable(Identifier.create("default", "compress_t"))
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
}
