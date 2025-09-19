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
import org.apache.paimon.fs.Path
import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.Row

class PaimonFormatTableReadITCase extends PaimonSparkTestWithRestCatalogBase {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("USE paimon")
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("USE test_db")
  }

  test("FormatTable: write and read non-partitioned table") {
    for { (format, compression) <- Seq(("csv", "none"), ("json", "none"), ("parquet", "zstd")) } {
      val tableName = s"format_test_$format"
      withTable(tableName) {
        // Create format table using the same pattern as FormatTableTestBase
        sql(s"CREATE TABLE $tableName (id INT, name STRING, value DOUBLE) USING $format " +
          s"TBLPROPERTIES ('type'='format-table', 'read.format-table.usePaimon'='true','file.compression'='$compression')")
        val paimonTable = paimonCatalog.getTable(Identifier.create("test_db", tableName))
        val path =
          paimonCatalog.getTable(Identifier.create("test_db", tableName)).options().get("path")
        fileIO.mkdirs(new Path(path))
        // Insert data using our new write implementation
        sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 10.5)")
        sql(s"INSERT INTO $tableName VALUES (2, 'Bob', 20.7)")
        sql(s"INSERT INTO $tableName VALUES (3, 'Charlie', 30.9)")

        // Test reading all data
        checkAnswer(
          sql(s"SELECT * FROM $tableName ORDER BY id"),
          Seq(
            Row(1, "Alice", 10.5),
            Row(2, "Bob", 20.7),
            Row(3, "Charlie", 30.9)
          )
        )

        // Test column projection (using our scan builder)
        checkAnswer(
          sql(s"SELECT name, value FROM $tableName WHERE id = 2"),
          Seq(Row("Bob", 20.7))
        )

        // Test filtering
        checkAnswer(
          sql(s"SELECT id FROM $tableName WHERE value > 15.0 ORDER BY id"),
          Seq(Row(2), Row(3))
        )

        // Verify this is actually a FormatTable
        val table = paimonCatalog.getTable(Identifier.create("test_db", tableName))
        assert(
          table.isInstanceOf[FormatTable],
          s"Table should be FormatTable but was ${table.getClass}")
        sql(s"DROP TABLE $tableName")
      }
    }
  }

  test("FormatTable: write and read partitioned table") {
    for { (format, compression) <- Seq(("parquet", "zstd")) } {
      val tableName = s"format_test_partitioned_$format"
      withTable(tableName) {
        // Create partitioned format table
        sql(
          s"CREATE TABLE $tableName (id INT, name STRING, value DOUBLE, dept STRING) USING $format " +
            s"PARTITIONED BY (dept) TBLPROPERTIES ('type'='format-table', 'read.format-table.usePaimon'='true', 'file.compression'='$compression')")
        val paimonTable = paimonCatalog.getTable(Identifier.create("test_db", tableName))
        val path =
          paimonCatalog.getTable(Identifier.create("test_db", tableName)).options().get("path")
        fileIO.mkdirs(new Path(path))
        // Insert data into different partitions
        sql(
          s"INSERT INTO $tableName VALUES (1, 'Alice', 10.5, 'Engineering')," +
            s" (2, 'Bob', 20.7, 'Engineering')," +
            s" (3, 'Charlie', 30.9, 'Sales')," +
            s" (4, 'David', 25.3, 'Sales')," +
            s" (5, 'Eve', 15.8, 'Marketing')")

        // Test reading all data
        checkAnswer(
          sql(s"SELECT * FROM $tableName ORDER BY id"),
          Seq(
            Row(1, "Alice", 10.5, "Engineering"),
            Row(2, "Bob", 20.7, "Engineering"),
            Row(3, "Charlie", 30.9, "Sales"),
            Row(4, "David", 25.3, "Sales"),
            Row(5, "Eve", 15.8, "Marketing")
          )
        )

        // Test partition filtering
        checkAnswer(
          sql(s"SELECT * FROM $tableName WHERE dept = 'Engineering' ORDER BY id"),
          Seq(
            Row(1, "Alice", 10.5, "Engineering"),
            Row(2, "Bob", 20.7, "Engineering")
          )
        )

        // Test column projection with partition filtering
        val result =
          sql(s"SELECT  name, value  FROM $tableName WHERE dept = 'Sales' ORDER BY id").collect()
        checkAnswer(
          sql(s"SELECT name, value FROM $tableName WHERE dept = 'Sales' ORDER BY id"),
          Seq(
            Row("Charlie", 30.9),
            Row("David", 25.3)
          )
        )

        // Test filtering on non-partition columns
        checkAnswer(
          sql(s"SELECT id, dept FROM $tableName WHERE value > 20.0 ORDER BY id"),
          Seq(
            Row(2, "Engineering"),
            Row(3, "Sales"),
            Row(4, "Sales")
          )
        )

        // Test combined filtering (partition + non-partition columns)
        checkAnswer(
          sql(s"SELECT name FROM $tableName WHERE dept = 'Sales' AND value > 25.0"),
          Seq(Row("Charlie"), Row("David"))
        )
        sql(s"DROP TABLE $tableName")
      }
    }
  }
}
