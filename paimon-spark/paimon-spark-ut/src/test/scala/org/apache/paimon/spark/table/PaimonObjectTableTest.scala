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

package org.apache.paimon.spark.table

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.spark.read.ObjectTableScan
import org.apache.paimon.table.`object`.ObjectTable

import org.apache.spark.sql.Row

class PaimonObjectTableTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("USE paimon")
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("USE test_db")
  }

  test("ObjectTable: read file metadata") {
    withObjectTable("object_table_metadata_test") {
      tableName =>
        checkAnswer(
          sql(s"SELECT path, name FROM $tableName ORDER BY path"),
          Seq(
            Row("file1.txt", "file1.txt"),
            Row("file2.txt", "file2.txt"),
            Row("subdir/file3.txt", "file3.txt"))
        )

        // Verify schema has expected columns
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tableName"),
          Seq(Row(3))
        )
    }
  }

  test("ObjectTable: push down filters") {
    withObjectTable("object_table_filter_pushdown_test") {
      tableName =>
        val filteredQuery =
          s"SELECT path FROM $tableName WHERE name = 'file2.txt' AND length > 0"
        checkAnswer(sql(filteredQuery), Seq(Row("file2.txt")))
        assert(getScan(filteredQuery).asInstanceOf[ObjectTableScan].pushedDataFilters.size == 2)
    }
  }

  test("ObjectTable: retain unsupported filters for post-scan evaluation") {
    withObjectTable("object_table_post_scan_filter_test") {
      tableName =>
        val filteredQuery =
          s"SELECT path FROM $tableName WHERE reverse(name) = 'txt.2elif'"
        checkAnswer(sql(filteredQuery), Seq(Row("file2.txt")))
        assert(getScan(filteredQuery).asInstanceOf[ObjectTableScan].pushedDataFilters.isEmpty)
    }
  }

  private def withObjectTable(tableName: String)(testCode: String => Unit): Unit = {
    withTable(tableName) {
      sql(
        s"CREATE TABLE $tableName TBLPROPERTIES (" +
          s"'type' = 'object-table')")

      val objectTable =
        paimonCatalog
          .getTable(Identifier.create("test_db", tableName))
          .asInstanceOf[ObjectTable]

      val fileIO = objectTable.fileIO()
      val basePath = new Path(objectTable.location())
      fileIO.mkdirs(basePath)
      fileIO.writeFile(new Path(basePath, "file1.txt"), "content1", false)
      fileIO.writeFile(new Path(basePath, "file2.txt"), "content2", false)
      fileIO.mkdirs(new Path(basePath, "subdir"))
      fileIO.writeFile(new Path(basePath, "subdir/file3.txt"), "content3", false)

      testCode(tableName)
    }
  }
}
