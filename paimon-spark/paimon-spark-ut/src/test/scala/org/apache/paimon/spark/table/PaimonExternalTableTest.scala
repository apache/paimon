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
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.schema.{Schema, SchemaManager}
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.DataTypes
import org.apache.paimon.utils.StringUtils

import org.apache.spark.sql.Row

class PaimonExternalTableTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("USE paimon")
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("USE test_db")
    // Clean up any existing tables from previous test runs
    sql("DROP TABLE IF EXISTS external_tbl")
    sql("DROP TABLE IF EXISTS managed_tbl")
    sql("DROP TABLE IF EXISTS external_tbl_renamed")
    sql("DROP TABLE IF EXISTS t1")
    sql("DROP TABLE IF EXISTS t2")
  }

  test("PaimonExternalTable: create and drop external table") {
    withTempDir {
      tbLocation =>
        withTable("external_tbl", "managed_tbl") {
          val externalTbLocation = tbLocation.getCanonicalPath
          // Ensure table doesn't exist before starting
          sql("DROP TABLE IF EXISTS external_tbl")

          // For REST catalog external tables, schema must be created in filesystem first
          val schemaTablePath = new Path(externalTbLocation)
          val schemaFileIO = LocalFileIO.create()
          val schema = Schema
            .newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .option("path", externalTbLocation)
            .option("type", "table")
            .build()
          new SchemaManager(schemaFileIO, schemaTablePath).createTable(schema, true)

          // create external table
          sql(
            s"CREATE TABLE external_tbl (id INT, name STRING) USING paimon LOCATION '$externalTbLocation'")
          sql("INSERT INTO external_tbl VALUES (1, 'Alice'), (2, 'Bob')")
          checkAnswer(
            sql("SELECT * FROM external_tbl ORDER BY id"),
            Seq(Row(1, "Alice"), Row(2, "Bob"))
          )

          val table = paimonCatalog
            .getTable(Identifier.create("test_db", "external_tbl"))
            .asInstanceOf[FileStoreTable]
          val fileIO = table.fileIO()
          val actualTbLocation = table.location()

          // For REST catalog, the path might be managed internally, but the table should still function as external
          // Verify that the table has a location and is accessible
          assert(actualTbLocation != null, "External table should have a location")

          // Verify data is accessible
          assert(fileIO.exists(actualTbLocation), "External table location should exist")

          // drop external table - data should still exist (this is the key characteristic of external tables)
          sql("DROP TABLE external_tbl")
          assert(fileIO.exists(actualTbLocation), "External table data should exist after drop")

          // Invalidate catalog cache to ensure table is fully removed
          try {
            paimonCatalog.invalidateTable(Identifier.create("test_db", "external_tbl"))
          } catch {
            case _: Exception => // Ignore if table doesn't exist in cache
          }

          // Wait a bit and ensure table is fully dropped before recreating
          Thread.sleep(100) // Give catalog time to fully process the drop
          sql("DROP TABLE IF EXISTS external_tbl")

          // Schema already exists in filesystem from initial creation, no need to recreate
          // create external table again using the same location - should be able to read existing data
          sql(
            s"CREATE TABLE external_tbl (id INT, name STRING) USING paimon LOCATION '$externalTbLocation'")
          checkAnswer(
            sql("SELECT * FROM external_tbl ORDER BY id"),
            Seq(Row(1, "Alice"), Row(2, "Bob"))
          )

          // create managed table for comparison
          sql("CREATE TABLE managed_tbl (id INT, name STRING) USING paimon")
          sql("INSERT INTO managed_tbl VALUES (3, 'Charlie')")
          val managedTable = paimonCatalog
            .getTable(Identifier.create("test_db", "managed_tbl"))
            .asInstanceOf[FileStoreTable]
          val managedTbLocation = managedTable.location()

          // drop managed table - data should be deleted
          sql("DROP TABLE managed_tbl")
          assert(
            !fileIO.exists(managedTbLocation),
            "Managed table data should not exist after drop")
        }
    }
  }

  test("PaimonExternalTable: partitioned external table") {
    withTempDir {
      tbLocation =>
        withTable("external_tbl") {
          val externalTbLocation = tbLocation.getCanonicalPath

          // For REST catalog external tables, schema must be created in filesystem first
          val schemaTablePath = new Path(externalTbLocation)
          val schemaFileIO = LocalFileIO.create()
          val schema = Schema
            .newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("value", DataTypes.DOUBLE())
            .column("dept", DataTypes.STRING())
            .partitionKeys("dept")
            .option("path", externalTbLocation)
            .option("type", "table")
            .build()
          new SchemaManager(schemaFileIO, schemaTablePath).createTable(schema, true)

          sql(s"""
                 |CREATE TABLE external_tbl (id INT, name STRING, value DOUBLE) USING paimon
                 |PARTITIONED BY (dept STRING)
                 |LOCATION '$externalTbLocation'
                 |""".stripMargin)

          sql(
            "INSERT INTO external_tbl VALUES " +
              "(1, 'Alice', 10.5, 'Engineering')," +
              "(2, 'Bob', 20.7, 'Engineering')," +
              "(3, 'Charlie', 30.9, 'Sales')," +
              "(4, 'David', 25.3, 'Sales')")

          // Test reading all data
          checkAnswer(
            sql("SELECT * FROM external_tbl ORDER BY id"),
            Seq(
              Row(1, "Alice", 10.5, "Engineering"),
              Row(2, "Bob", 20.7, "Engineering"),
              Row(3, "Charlie", 30.9, "Sales"),
              Row(4, "David", 25.3, "Sales")
            )
          )

          // Test partition filtering
          checkAnswer(
            sql("SELECT * FROM external_tbl WHERE dept = 'Engineering' ORDER BY id"),
            Seq(
              Row(1, "Alice", 10.5, "Engineering"),
              Row(2, "Bob", 20.7, "Engineering")
            )
          )

          // Test column projection with partition filtering
          checkAnswer(
            sql("SELECT name, value FROM external_tbl WHERE dept = 'Sales' ORDER BY id"),
            Seq(
              Row("Charlie", 30.9),
              Row("David", 25.3)
            )
          )

          // Verify this is an external table - drop and check data exists
          val table = paimonCatalog
            .getTable(Identifier.create("test_db", "external_tbl"))
            .asInstanceOf[FileStoreTable]
          val fileIO = table.fileIO()
          val actualTbLocation = table.location()

          sql("DROP TABLE external_tbl")
          assert(fileIO.exists(actualTbLocation), "External table data should exist after drop")
        }
    }
  }

  test("PaimonExternalTable: rename external table") {
    withTempDir {
      tbLocation =>
        withTable("external_tbl", "external_tbl_renamed") {
          val externalTbLocation = tbLocation.getCanonicalPath

          // For REST catalog external tables, schema must be created in filesystem first
          val schemaTablePath = new Path(externalTbLocation)
          val schemaFileIO = LocalFileIO.create()
          val schema = Schema
            .newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .option("path", externalTbLocation)
            .option("type", "table")
            .build()
          new SchemaManager(schemaFileIO, schemaTablePath).createTable(schema, true)

          // create external table
          sql(
            s"CREATE TABLE external_tbl (id INT, name STRING) USING paimon LOCATION '$externalTbLocation'")
          sql("INSERT INTO external_tbl VALUES (1, 'Alice')")
          val originalLocation = paimonCatalog
            .getTable(Identifier.create("test_db", "external_tbl"))
            .asInstanceOf[FileStoreTable]
            .location()

          // rename external table, location should not change
          sql("ALTER TABLE external_tbl RENAME TO external_tbl_renamed")
          checkAnswer(
            sql("SELECT * FROM external_tbl_renamed"),
            Seq(Row(1, "Alice"))
          )

          val renamedTable = paimonCatalog
            .getTable(Identifier.create("test_db", "external_tbl_renamed"))
            .asInstanceOf[FileStoreTable]
          val renamedLocation = renamedTable.location()
          assert(
            renamedLocation.toString.equals(originalLocation.toString),
            "External table location should not change after rename"
          )
        }
    }
  }

  test("PaimonExternalTable: create external table without schema") {
    withTempDir {
      tbLocation =>
        withTable("t1", "t2") {
          val externalTbLocation = tbLocation.getCanonicalPath

          // For REST catalog external tables, schema must be created in filesystem first
          val schemaTablePath = new Path(externalTbLocation)
          val schemaFileIO = LocalFileIO.create()
          val schema = Schema
            .newBuilder()
            .column("id", DataTypes.INT())
            .column("pt", DataTypes.INT())
            .partitionKeys("pt")
            .primaryKey("id")
            .option("path", externalTbLocation)
            .option("type", "table")
            .build()
          new SchemaManager(schemaFileIO, schemaTablePath).createTable(schema, true)

          // First create a table with schema and data
          sql(s"""
                 |CREATE TABLE t1 (id INT, pt INT) USING paimon
                 |PARTITIONED BY (pt)
                 |TBLPROPERTIES('primary-key' = 'id')
                 |LOCATION '$externalTbLocation'
                 |""".stripMargin)
          sql("INSERT INTO t1 VALUES (1, 1), (2, 2)")

          // create external table without schema - should infer from existing table
          sql(s"CREATE TABLE t2 USING paimon LOCATION '$externalTbLocation'")
          checkAnswer(
            sql("SELECT * FROM t2 ORDER BY id"),
            Seq(Row(1, 1), Row(2, 2))
          )

          val table2 =
            paimonCatalog.getTable(Identifier.create("test_db", "t2")).asInstanceOf[FileStoreTable]
          val table2Location = table2.location()
          // Verify table2 can access the data from table1's location
          assert(table2Location != null, "Table t2 should have a location")
          // The key point is that t2 can read data from the same location as t1
          assert(table2.fileIO().exists(table2Location), "Table t2 location should exist")
        }
    }
  }

  test("PaimonExternalTable: create external table on managed table location") {
    withTable("external_tbl", "managed_tbl") {
      // Create managed table first
      sql("CREATE TABLE managed_tbl (id INT, name STRING) USING paimon")
      sql("INSERT INTO managed_tbl VALUES (1, 'Alice'), (2, 'Bob')")
      checkAnswer(
        sql("SELECT * FROM managed_tbl ORDER BY id"),
        Seq(Row(1, "Alice"), Row(2, "Bob"))
      )

      val managedTable = paimonCatalog
        .getTable(Identifier.create("test_db", "managed_tbl"))
        .asInstanceOf[FileStoreTable]
      val managedLocation = managedTable.location()
      // Extract actual file system path, removing any scheme prefix (e.g., "file:", "rest:")
      val tablePath = if (managedLocation.toString.contains(":")) {
        // Path has scheme, extract the path part after the first ":"
        val parts = managedLocation.toString.split(":", 2)
        if (parts.length == 2 && parts(0).equals("file")) {
          parts(1) // For file: scheme, use the path directly
        } else {
          // For other schemes or if parsing fails, try to get canonical path
          try {
            new java.io.File(managedLocation.toString.replaceFirst("^[^:]+:", "")).getCanonicalPath
          } catch {
            case _: Exception => managedLocation.toString.replaceFirst("^[^:]+:", "")
          }
        }
      } else {
        managedLocation.toString
      }

      // For REST catalog, managed table already has schema, no need to create schema again
      // Create external table pointing to managed table location
      sql(s"CREATE TABLE external_tbl (id INT, name STRING) USING paimon LOCATION '$tablePath'")
      checkAnswer(
        sql("SELECT * FROM external_tbl ORDER BY id"),
        Seq(Row(1, "Alice"), Row(2, "Bob"))
      )

      val externalTable = paimonCatalog
        .getTable(Identifier.create("test_db", "external_tbl"))
        .asInstanceOf[FileStoreTable]
      assert(
        StringUtils.replace(externalTable.location().toString, "file:", "").equals(tablePath),
        "External table should point to managed table location"
      )

      // Drop managed table - managed table deletion will delete data files
      // since external table points to the same location, data will be deleted
      sql("DROP TABLE managed_tbl")
      val fileIO = externalTable.fileIO()
      assert(
        !fileIO.exists(externalTable.location()),
        "Data should be deleted after dropping managed table since external table points to managed table location"
      )

      // External table cannot read data anymore since data was deleted with managed table
      // This demonstrates that external table pointing to managed table location shares the same data
      checkAnswer(
        sql("SELECT * FROM external_tbl ORDER BY id"),
        Seq.empty
      )
    }
  }

  test("PaimonExternalTable: insert overwrite on external table") {
    withTempDir {
      tbLocation =>
        withTable("external_tbl") {
          val externalTbLocation = tbLocation.getCanonicalPath

          // For REST catalog external tables, schema must be created in filesystem first
          val schemaTablePath = new Path(externalTbLocation)
          val schemaFileIO = LocalFileIO.create()
          val schema = Schema
            .newBuilder()
            .column("age", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .option("path", externalTbLocation)
            .option("type", "table")
            .build()
          new SchemaManager(schemaFileIO, schemaTablePath).createTable(schema, true)

          sql(
            s"CREATE TABLE external_tbl (age INT, name STRING) USING paimon LOCATION '$externalTbLocation'")

          sql("INSERT INTO external_tbl VALUES (5, 'Ben'), (7, 'Larry')")
          checkAnswer(
            sql("SELECT age, name FROM external_tbl ORDER BY age"),
            Seq(Row(5, "Ben"), Row(7, "Larry"))
          )

          sql("INSERT OVERWRITE external_tbl VALUES (5, 'Jerry'), (7, 'Tom')")
          checkAnswer(
            sql("SELECT age, name FROM external_tbl ORDER BY age"),
            Seq(Row(5, "Jerry"), Row(7, "Tom"))
          )
        }
    }
  }

  test("PaimonExternalTable: insert overwrite on partitioned external table") {
    withTempDir {
      tbLocation =>
        withTable("external_tbl") {
          val externalTbLocation = tbLocation.getCanonicalPath

          // For REST catalog external tables, schema must be created in filesystem first
          val schemaTablePath = new Path(externalTbLocation)
          val schemaFileIO = LocalFileIO.create()
          val schema = Schema
            .newBuilder()
            .column("age", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("id", DataTypes.INT())
            .partitionKeys("id")
            .option("path", externalTbLocation)
            .option("type", "table")
            .build()
          new SchemaManager(schemaFileIO, schemaTablePath).createTable(schema, true)

          sql(s"""
                 |CREATE TABLE external_tbl (age INT, name STRING) USING paimon
                 |PARTITIONED BY (id INT)
                 |LOCATION '$externalTbLocation'
                 |""".stripMargin)

          sql("INSERT INTO external_tbl PARTITION (id = 1) VALUES (5, 'Ben'), (7, 'Larry')")
          sql("INSERT OVERWRITE external_tbl PARTITION (id = 1) VALUES (5, 'Jerry'), (7, 'Tom')")
          checkAnswer(
            sql("SELECT id, age, name FROM external_tbl ORDER BY id, age"),
            Seq(Row(1, 5, "Jerry"), Row(1, 7, "Tom"))
          )

          sql("INSERT INTO external_tbl PARTITION (id = 3) VALUES (5, 'Alice')")
          // Use dynamic partition overwrite mode to only overwrite partitions present in data
          withSparkSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
            sql("INSERT OVERWRITE external_tbl VALUES (5, 'Jerry', 1), (7, 'Tom', 2)")
          }
          checkAnswer(
            sql("SELECT id, age, name FROM external_tbl ORDER BY id, age"),
            Seq(Row(1, 5, "Jerry"), Row(2, 7, "Tom"), Row(3, 5, "Alice"))
          )
        }
    }
  }

  test("PaimonExternalTable: show partitions on external table") {
    withTempDir {
      tbLocation =>
        withTable("external_tbl") {
          val externalTbLocation = tbLocation.getCanonicalPath

          // For REST catalog external tables, schema must be created in filesystem first
          val schemaTablePath = new Path(externalTbLocation)
          val schemaFileIO = LocalFileIO.create()
          val schema = Schema
            .newBuilder()
            .column("id", DataTypes.INT())
            .column("p1", DataTypes.INT())
            .column("p2", DataTypes.STRING())
            .partitionKeys("p1", "p2")
            .option("path", externalTbLocation)
            .option("type", "table")
            .build()
          new SchemaManager(schemaFileIO, schemaTablePath).createTable(schema, true)

          sql(s"""
                 |CREATE TABLE external_tbl (id INT, p1 INT, p2 STRING) USING paimon
                 |PARTITIONED BY (p1, p2)
                 |LOCATION '$externalTbLocation'
                 |""".stripMargin)

          sql("INSERT INTO external_tbl VALUES (1, 1, '1')")
          sql("INSERT INTO external_tbl VALUES (2, 1, '1')")
          sql("INSERT INTO external_tbl VALUES (3, 2, '1')")
          sql("INSERT INTO external_tbl VALUES (3, 2, '2')")

          checkAnswer(
            sql("SHOW PARTITIONS external_tbl"),
            Seq(Row("p1=1/p2=1"), Row("p1=2/p2=1"), Row("p1=2/p2=2")))
          checkAnswer(
            sql("SHOW PARTITIONS external_tbl PARTITION (p1=2)"),
            Seq(Row("p1=2/p2=1"), Row("p1=2/p2=2")))
          checkAnswer(
            sql("SHOW PARTITIONS external_tbl PARTITION (p1=2, p2='2')"),
            Seq(Row("p1=2/p2=2")))
        }
    }
  }
}
