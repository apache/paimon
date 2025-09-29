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

import org.apache.paimon.CoreOptions
import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.Row

class PaimonFormatTableTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("USE paimon")
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("USE test_db")
  }

  test("PaimonFormatTableRead table: csv with field-delimiter") {
    val tableName = "paimon_format_test_csv_options"
    withTable(tableName) {
      sql(
        s"CREATE TABLE $tableName (f0 INT, f1 string) USING CSV OPTIONS ('" +
          s"file.compression'='none', 'seq'='|', 'lineSep'='\n', " +
          s"'${CoreOptions.FORMAT_TABLE_IMPLEMENTATION
              .key()}'='${CoreOptions.FormatTableImplementation.PAIMON.toString}') PARTITIONED BY (`ds` bigint)")
      val table =
        paimonCatalog.getTable(Identifier.create("test_db", tableName)).asInstanceOf[FormatTable]
      val partition = 20250920
      val csvFile =
        new Path(
          table.location(),
          s"ds=$partition/part-00000-0a28422e-68ba-4713-8870-2fde2d36ed06-c001.csv")
      table.fileIO().writeFile(csvFile, "1|asfasdfsdf\n2|asfasdfsdf", false)
      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(Row(1, "asfasdfsdf", partition), Row(2, "asfasdfsdf", partition)))
    }
  }

  test("PaimonFormatTableRead: read non-partitioned table") {
    for {
      (format, compression) <- Seq(
        ("csv", "gzip"),
        ("json", "gzip"),
        ("parquet", "zstd"),
        ("orc", "zstd"))
    } {
      val tableName = s"format_test_$format"
      withTable(tableName) {
        // Create format table using the same pattern as FormatTableTestBase
        sql(
          s"CREATE TABLE $tableName (id INT, name STRING, value DOUBLE) USING $format " +
            s"TBLPROPERTIES ('file.compression'='$compression', 'seq'=',', 'lineSep'='\n')")
        val path =
          paimonCatalog.getTable(Identifier.create("test_db", tableName)).options().get("path")
        fileIO.mkdirs(new Path(path))
        // Insert data using our new write implementation
        sql(s"INSERT INTO $tableName VALUES (1, 'Alice', 10.5)")
        sql(s"INSERT INTO $tableName VALUES (2, 'Bob', 20.7)")
        sql(s"INSERT INTO $tableName VALUES (3, 'Charlie', 30.9)")

        // Test reading all data
        sql(
          s"Alter table $tableName SET TBLPROPERTIES ('${CoreOptions.FORMAT_TABLE_IMPLEMENTATION.key()}'" +
            s"='${CoreOptions.FormatTableImplementation.PAIMON.toString}')")
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

  test("PaimonFormatTableRead: read partitioned table") {
    for {
      (format, compression) <- Seq(
        ("csv", "gzip"),
        ("json", "gzip"),
        ("parquet", "zstd"),
        ("orc", "zstd"))
    } {
      val tableName = s"format_test_partitioned_$format"
      withTable(tableName) {
        // Create partitioned format table
        sql(
          s"CREATE TABLE $tableName (id INT, name STRING, value DOUBLE, dept STRING) USING $format " +
            s"PARTITIONED BY (dept) TBLPROPERTIES ('file.compression'='$compression')")
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
        sql(
          s"Alter table $tableName SET TBLPROPERTIES ('${CoreOptions.FORMAT_TABLE_IMPLEMENTATION.key()}'" +
            s"='${CoreOptions.FormatTableImplementation.PAIMON.toString}')")
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
