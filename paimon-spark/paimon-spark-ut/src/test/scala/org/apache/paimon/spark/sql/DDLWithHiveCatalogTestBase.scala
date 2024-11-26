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

import org.apache.paimon.hive.HiveMetastoreClient
import org.apache.paimon.spark.PaimonHiveTestBase
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions

abstract class DDLWithHiveCatalogTestBase extends PaimonHiveTestBase {

  test("Paimon DDL with hive catalog: create database with location and comment") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        spark.sql(s"USE $catalogName")
        withTempDir {
          dBLocation =>
            withDatabase("paimon_db") {
              val comment = "this is a test comment"
              spark.sql(
                s"CREATE DATABASE paimon_db LOCATION '${dBLocation.getCanonicalPath}' COMMENT '$comment'")
              Assertions.assertEquals(getDatabaseLocation("paimon_db"), dBLocation.getCanonicalPath)
              Assertions.assertEquals(getDatabaseComment("paimon_db"), comment)

              withTable("paimon_db.paimon_tbl") {
                spark.sql(s"""
                             |CREATE TABLE paimon_db.paimon_tbl (id STRING, name STRING, pt STRING)
                             |USING PAIMON
                             |TBLPROPERTIES ('primary-key' = 'id')
                             |""".stripMargin)
                Assertions.assertEquals(
                  getTableLocation("paimon_db.paimon_tbl"),
                  s"${dBLocation.getCanonicalPath}/paimon_tbl")

                val fileStoreTable = getPaimonScan("SELECT * FROM paimon_db.paimon_tbl").table
                  .asInstanceOf[FileStoreTable]
                Assertions.assertEquals("paimon_tbl", fileStoreTable.name())
                Assertions.assertEquals("paimon_db.paimon_tbl", fileStoreTable.fullName())
              }
            }
        }
    }
  }

  test("Paimon DDL with hive catalog: drop partition for paimon table sparkCatalogName") {
    Seq(paimonHiveCatalogName).foreach {
      catalogName =>
        spark.sql(s"USE $catalogName")
        withTempDir {
          dBLocation =>
            withDatabase("paimon_db") {
              val comment = "this is a test comment"
              spark.sql(
                s"CREATE DATABASE paimon_db LOCATION '${dBLocation.getCanonicalPath}' COMMENT '$comment'")
              Assertions.assertEquals(getDatabaseLocation("paimon_db"), dBLocation.getCanonicalPath)
              Assertions.assertEquals(getDatabaseComment("paimon_db"), comment)

              withTable("paimon_db.paimon_tbl") {
                spark.sql(s"""
                             |CREATE TABLE paimon_db.paimon_tbl (id STRING, name STRING, pt STRING)
                             |USING PAIMON
                             |PARTITIONED BY (name, pt)
                             |TBLPROPERTIES('metastore.partitioned-table' = 'true')
                             |""".stripMargin)
                Assertions.assertEquals(
                  getTableLocation("paimon_db.paimon_tbl"),
                  s"${dBLocation.getCanonicalPath}/paimon_tbl")
                spark.sql("insert into paimon_db.paimon_tbl select '1', 'n', 'cc'")
                spark.sql("insert into paimon_db.paimon_tbl select '1', 'n1', 'aa'")
                spark.sql("insert into paimon_db.paimon_tbl select '1', 'n2', 'bb'")

                spark.sql("show partitions paimon_db.paimon_tbl")
                checkAnswer(
                  spark.sql("show partitions paimon_db.paimon_tbl"),
                  Row("name=n/pt=cc") :: Row("name=n1/pt=aa") :: Row("name=n2/pt=bb") :: Nil)
                spark.sql(
                  "alter table paimon_db.paimon_tbl drop partition (name='n1', `pt`='aa'), partition (name='n2', `pt`='bb')")
                spark.sql("show partitions paimon_db.paimon_tbl")
                checkAnswer(
                  spark.sql("show partitions paimon_db.paimon_tbl"),
                  Row("name=n/pt=cc") :: Nil)

              }

              // disable metastore.partitioned-table
              withTable("paimon_db.paimon_tbl2") {
                spark.sql(s"""
                             |CREATE TABLE paimon_db.paimon_tbl2 (id STRING, name STRING, pt STRING)
                             |USING PAIMON
                             |PARTITIONED BY (name, pt)
                             |TBLPROPERTIES('metastore.partitioned-table' = 'false')
                             |""".stripMargin)
                Assertions.assertEquals(
                  getTableLocation("paimon_db.paimon_tbl2"),
                  s"${dBLocation.getCanonicalPath}/paimon_tbl2")
                spark.sql("insert into paimon_db.paimon_tbl2 select '1', 'n', 'cc'")
                spark.sql("insert into paimon_db.paimon_tbl2 select '1', 'n1', 'aa'")
                spark.sql("insert into paimon_db.paimon_tbl2 select '1', 'n2', 'bb'")

                spark.sql("show partitions paimon_db.paimon_tbl2")
                checkAnswer(
                  spark.sql("show partitions paimon_db.paimon_tbl2"),
                  Row("name=n/pt=cc") :: Row("name=n1/pt=aa") :: Row("name=n2/pt=bb") :: Nil)
                spark.sql(
                  "alter table paimon_db.paimon_tbl2 drop partition (name='n1', `pt`='aa'), partition (name='n2', `pt`='bb')")
                spark.sql("show partitions paimon_db.paimon_tbl2")
                checkAnswer(
                  spark.sql("show partitions paimon_db.paimon_tbl2"),
                  Row("name=n/pt=cc") :: Nil)

              }
            }
        }
    }
  }

  test("Paimon DDL with hive catalog: create partition for paimon table sparkCatalogName") {
    Seq(paimonHiveCatalogName).foreach {
      catalogName =>
        spark.sql(s"USE $catalogName")
        withTempDir {
          dBLocation =>
            withDatabase("paimon_db") {
              val comment = "this is a test comment"
              spark.sql(
                s"CREATE DATABASE paimon_db LOCATION '${dBLocation.getCanonicalPath}' COMMENT '$comment'")
              Assertions.assertEquals(getDatabaseLocation("paimon_db"), dBLocation.getCanonicalPath)
              Assertions.assertEquals(getDatabaseComment("paimon_db"), comment)

              withTable("paimon_db.paimon_tbl") {
                spark.sql(s"""
                             |CREATE TABLE paimon_db.paimon_tbl (id STRING, name STRING, pt STRING)
                             |USING PAIMON
                             |PARTITIONED BY (name, pt)
                             |TBLPROPERTIES('metastore.partitioned-table' = 'true')
                             |""".stripMargin)
                Assertions.assertEquals(
                  getTableLocation("paimon_db.paimon_tbl"),
                  s"${dBLocation.getCanonicalPath}/paimon_tbl")
                spark.sql("insert into paimon_db.paimon_tbl select '1', 'n', 'cc'")

                spark.sql("alter table paimon_db.paimon_tbl add partition(name='cc', `pt`='aa') ")
              }

              // disable metastore.partitioned-table
              withTable("paimon_db.paimon_tbl2") {
                spark.sql(s"""
                             |CREATE TABLE paimon_db.paimon_tbl2 (id STRING, name STRING, pt STRING)
                             |USING PAIMON
                             |PARTITIONED BY (name, pt)
                             |TBLPROPERTIES('metastore.partitioned-table' = 'false')
                             |""".stripMargin)
                Assertions.assertEquals(
                  getTableLocation("paimon_db.paimon_tbl2"),
                  s"${dBLocation.getCanonicalPath}/paimon_tbl2")
                spark.sql("insert into paimon_db.paimon_tbl2 select '1', 'n', 'cc'")

                spark.sql("alter table paimon_db.paimon_tbl2 add partition(name='cc', `pt`='aa') ")
              }
            }
        }
    }
  }

  test("Paimon DDL with hive catalog: create database with props") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        spark.sql(s"USE $catalogName")
        withDatabase("paimon_db") {
          spark.sql(s"CREATE DATABASE paimon_db WITH DBPROPERTIES ('k1' = 'v1', 'k2' = 'v2')")
          val props = getDatabaseProps("paimon_db")
          Assertions.assertEquals(props("k1"), "v1")
          Assertions.assertEquals(props("k2"), "v2")
          Assertions.assertTrue(getDatabaseOwner("paimon_db").nonEmpty)
        }
    }
  }

  test("Paimon DDL with hive catalog: set default database") {
    var reusedSpark = spark

    Seq("paimon", sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        {
          val dbName = s"${catalogName}_default_db"
          val tblName = s"${dbName}_tbl"

          reusedSpark.sql(s"use $catalogName")
          reusedSpark.sql(s"create database $dbName")
          reusedSpark.sql(s"use $dbName")
          reusedSpark.sql(s"create table $tblName (id int, name string, dt string) using paimon")
          reusedSpark.stop()

          reusedSpark = SparkSession
            .builder()
            .master("local[2]")
            .config(sparkConf)
            .config("spark.sql.defaultCatalog", catalogName)
            .config(s"spark.sql.catalog.$catalogName.defaultDatabase", dbName)
            .getOrCreate()

          if (catalogName.equals(sparkCatalogName) && !gteqSpark3_4) {
            checkAnswer(reusedSpark.sql("show tables").select("tableName"), Nil)
            reusedSpark.sql(s"use $dbName")
          }
          checkAnswer(reusedSpark.sql("show tables").select("tableName"), Row(tblName) :: Nil)

          reusedSpark.sql(s"drop table $tblName")
        }
    }

    // Since we created a new sparkContext, we need to stop it and reset the default sparkContext
    reusedSpark.stop()
    reset()
  }

  test("Paimon DDL with hive catalog: drop database cascade which contains paimon table") {
    // Spark supports DROP DATABASE CASCADE since 3.3
    if (gteqSpark3_3) {
      Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
        catalogName =>
          spark.sql(s"USE $catalogName")
          spark.sql(s"CREATE DATABASE paimon_db")
          spark.sql(s"USE paimon_db")
          spark.sql(s"CREATE TABLE paimon_tbl (id int, name string, dt string) using paimon")
          // Only spark_catalog supports create other table
          if (catalogName.equals(sparkCatalogName)) {
            spark.sql(s"CREATE TABLE parquet_tbl (id int, name string, dt string) using parquet")
            spark.sql(s"CREATE VIEW parquet_tbl_view AS SELECT * FROM parquet_tbl")
          }
          spark.sql(s"CREATE VIEW paimon_tbl_view AS SELECT * FROM paimon_tbl")
          spark.sql(s"USE default")
          spark.sql(s"DROP DATABASE paimon_db CASCADE")
      }
    }
  }

  test("Paimon DDL with hive catalog: sync partitions to HMS") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        val dbName = "default"
        val tblName = "t"
        spark.sql(s"USE $catalogName.$dbName")
        withTable(tblName) {
          spark.sql(s"""
                       |CREATE TABLE $tblName (id INT, pt INT)
                       |USING PAIMON
                       |TBLPROPERTIES ('metastore.partitioned-table' = 'true')
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          val metastoreClient = loadTable(dbName, tblName)
            .catalogEnvironment()
            .metastoreClientFactory()
            .create()
            .asInstanceOf[HiveMetastoreClient]
            .client()

          spark.sql(s"INSERT INTO $tblName VALUES (1, 1), (2, 2), (3, 3)")
          // check partitions in paimon
          checkAnswer(
            spark.sql(s"show partitions $tblName"),
            Seq(Row("pt=1"), Row("pt=2"), Row("pt=3")))
          // check partitions in HMS
          assert(metastoreClient.listPartitions(dbName, tblName, 100).size() == 3)

          spark.sql(s"INSERT INTO $tblName VALUES (4, 3), (5, 4)")
          checkAnswer(
            spark.sql(s"show partitions $tblName"),
            Seq(Row("pt=1"), Row("pt=2"), Row("pt=3"), Row("pt=4")))
          assert(metastoreClient.listPartitions(dbName, tblName, 100).size() == 4)

          spark.sql(s"ALTER TABLE $tblName DROP PARTITION (pt=1)")
          checkAnswer(
            spark.sql(s"show partitions $tblName"),
            Seq(Row("pt=2"), Row("pt=3"), Row("pt=4")))
          assert(metastoreClient.listPartitions(dbName, tblName, 100).size() == 3)
        }
    }
  }

  test("Paimon DDL with hive catalog: create and drop external / managed table") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        spark.sql(s"USE $catalogName")
        withTempDir {
          tbLocation =>
            withDatabase("paimon_db") {
              spark.sql(s"CREATE DATABASE IF NOT EXISTS paimon_db")
              spark.sql(s"USE paimon_db")
              withTable("external_tbl", "managed_tbl") {
                val expertTbLocation = tbLocation.getCanonicalPath
                // create external table
                spark.sql(
                  s"CREATE TABLE external_tbl (id INT) USING paimon LOCATION '$expertTbLocation'")
                spark.sql("INSERT INTO external_tbl VALUES (1)")
                checkAnswer(spark.sql("SELECT * FROM external_tbl"), Row(1))
                val table = loadTable("paimon_db", "external_tbl")
                val fileIO = table.fileIO()
                val actualTbLocation = table.location()
                assert(actualTbLocation.toString.split(':').apply(1).equals(expertTbLocation))

                // drop external table
                spark.sql("DROP TABLE external_tbl")
                assert(fileIO.exists(actualTbLocation))

                // create external table again using the same location
                spark.sql(
                  s"CREATE TABLE external_tbl (id INT) USING paimon LOCATION '$expertTbLocation'")
                checkAnswer(spark.sql("SELECT * FROM external_tbl"), Row(1))
                assert(
                  loadTable("paimon_db", "external_tbl")
                    .location()
                    .toString
                    .split(':')
                    .apply(1)
                    .equals(expertTbLocation))

                // create managed table
                spark.sql(s"CREATE TABLE managed_tbl (id INT) USING paimon")
                val managedTbLocation = loadTable("paimon_db", "managed_tbl").location()

                // drop managed table
                spark.sql("DROP TABLE managed_tbl")
                assert(!fileIO.exists(managedTbLocation))
              }
            }
        }
    }
  }

  test("Paimon DDL with hive catalog: rename external / managed table") {
    Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        spark.sql(s"USE $catalogName")
        withTempDir {
          tbLocation =>
            withDatabase("paimon_db") {
              spark.sql(s"CREATE DATABASE paimon_db")
              spark.sql(s"USE paimon_db")
              withTable(
                "external_tbl",
                "managed_tbl",
                "external_tbl_renamed",
                "managed_tbl_renamed") {
                val expertTbLocation = tbLocation.getCanonicalPath
                // create external table
                spark.sql(
                  s"CREATE TABLE external_tbl (id INT) USING paimon LOCATION '$expertTbLocation'")
                spark.sql("INSERT INTO external_tbl VALUES (1)")
                val actualTbLocation = loadTable("paimon_db", "external_tbl").location()
                assert(actualTbLocation.toString.split(':').apply(1).equals(expertTbLocation))

                // rename external table, location should not change
                spark.sql("ALTER TABLE external_tbl RENAME TO external_tbl_renamed")
                checkAnswer(spark.sql("SELECT * FROM external_tbl_renamed"), Row(1))
                assert(
                  loadTable("paimon_db", "external_tbl_renamed")
                    .location()
                    .toString
                    .split(':')
                    .apply(1)
                    .equals(expertTbLocation))

                // create managed table
                spark.sql(s"CREATE TABLE managed_tbl (id INT) USING paimon")
                spark.sql("INSERT INTO managed_tbl VALUES (1)")
                val managedTbLocation = loadTable("paimon_db", "managed_tbl").location()

                // rename managed table, location should change
                spark.sql("ALTER TABLE managed_tbl RENAME TO managed_tbl_renamed")
                checkAnswer(spark.sql("SELECT * FROM managed_tbl_renamed"), Row(1))
                assert(
                  !loadTable("paimon_db", "managed_tbl_renamed")
                    .location()
                    .toString
                    .split(':')
                    .apply(1)
                    .equals(managedTbLocation.toString))
              }
            }
        }
    }
  }

  def getDatabaseProp(dbName: String, propertyName: String): String = {
    spark
      .sql(s"DESC DATABASE EXTENDED $dbName")
      .filter(s"info_name == '$propertyName'")
      .head()
      .getAs[String]("info_value")
  }

  def getDatabaseLocation(dbName: String): String =
    getDatabaseProp(dbName, "Location").split(":")(1)

  def getDatabaseComment(dbName: String): String = getDatabaseProp(dbName, "Comment")

  def getDatabaseOwner(dbName: String): String = getDatabaseProp(dbName, "Owner")

  def getDatabaseProps(dbName: String): Map[String, String] = {
    val dbPropsStr = getDatabaseProp(dbName, "Properties")
    val pattern = "\\(([^,]+),([^)]+)\\)".r
    pattern
      .findAllIn(dbPropsStr.drop(1).dropRight(1))
      .matchData
      .map {
        m =>
          val key = m.group(1).trim
          val value = m.group(2).trim
          (key, value)
      }
      .toMap
  }

  def getTableLocation(tblName: String): String = {
    val tablePropsStr = spark
      .sql(s"DESC TABLE EXTENDED $tblName")
      .filter("col_name == 'Table Properties'")
      .head()
      .getAs[String]("data_type")
    val tableProps = tablePropsStr
      .substring(1, tablePropsStr.length - 1)
      .split(",")
      .map(_.split("="))
      .map { case Array(key, value) => (key, value) }
      .toMap
    tableProps("path").split(":")(1)
  }
}
