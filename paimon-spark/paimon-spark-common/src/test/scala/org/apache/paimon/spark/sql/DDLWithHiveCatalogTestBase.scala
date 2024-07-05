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

import org.apache.paimon.spark.PaimonHiveTestBase

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
          // Currently, only spark_catalog supports create other table or view
          if (catalogName.equals(sparkCatalogName)) {
            spark.sql(s"CREATE TABLE parquet_tbl (id int, name string, dt string) using parquet")
            spark.sql(s"CREATE VIEW parquet_tbl_view AS SELECT * FROM parquet_tbl")
            spark.sql(s"CREATE VIEW paimon_tbl_view AS SELECT * FROM paimon_tbl")
          }
          spark.sql(s"USE default")
          spark.sql(s"DROP DATABASE paimon_db CASCADE")
      }
    }
  }

  def getDatabaseLocation(dbName: String): String = {
    spark
      .sql(s"DESC DATABASE $dbName")
      .filter("info_name == 'Location'")
      .head()
      .getAs[String]("info_value")
      .split(":")(1)
  }

  def getDatabaseComment(dbName: String): String = {
    spark
      .sql(s"DESC DATABASE $dbName")
      .filter("info_name == 'Comment'")
      .head()
      .getAs[String]("info_value")
  }

  def getDatabaseProps(dbName: String): Map[String, String] = {
    val dbPropsStr = spark
      .sql(s"DESC DATABASE EXTENDED $dbName")
      .filter("info_name == 'Properties'")
      .head()
      .getAs[String]("info_value")
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
