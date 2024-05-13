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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.junit.jupiter.api.Assertions

abstract class DDLTestBase extends PaimonSparkTestBase {

  import testImplicits._

  test("Paimon DDL: Create Table As Select") {
    withTable("source", "t1", "t2") {
      Seq((1L, "x1", "2023"), (2L, "x2", "2023"))
        .toDF("a", "b", "pt")
        .createOrReplaceTempView("source")

      spark.sql("""
                  |CREATE TABLE t1 AS SELECT * FROM source
                  |""".stripMargin)
      val t1 = loadTable("t1")
      Assertions.assertTrue(t1.primaryKeys().isEmpty)
      Assertions.assertTrue(t1.partitionKeys().isEmpty)

      spark.sql(
        """
          |CREATE TABLE t2
          |PARTITIONED BY (pt)
          |TBLPROPERTIES ('bucket' = '5', 'primary-key' = 'a,pt', 'target-file-size' = '128MB')
          |AS SELECT * FROM source
          |""".stripMargin)
      val t2 = loadTable("t2")
      Assertions.assertEquals(2, t2.primaryKeys().size())
      Assertions.assertTrue(t2.primaryKeys().contains("a"))
      Assertions.assertTrue(t2.primaryKeys().contains("pt"))
      Assertions.assertEquals(1, t2.partitionKeys().size())
      Assertions.assertEquals("pt", t2.partitionKeys().get(0))

      // check all the core options
      Assertions.assertEquals("5", t2.options().get("bucket"))
      Assertions.assertEquals("128MB", t2.options().get("target-file-size"))
    }
  }

  test("Paimon DDL: create database with location with filesystem catalog") {
    withTempDir {
      dBLocation =>
        withDatabase("paimon_db") {
          val error = intercept[Exception] {
            spark.sql(s"CREATE DATABASE paimon_db LOCATION '${dBLocation.getCanonicalPath}'")
          }.getMessage
          assert(
            error.contains("Cannot specify location for a database when using fileSystem catalog."))
        }
    }
  }

  test("Paimon DDL: create other table with paimon SparkCatalog") {
    withTable("paimon_tbl1", "paimon_tbl2", "parquet_tbl") {
      spark.sql(s"CREATE TABLE paimon_tbl1 (id int) USING paimon")
      spark.sql(s"CREATE TABLE paimon_tbl2 (id int)")
      val error = intercept[Exception] {
        spark.sql(s"CREATE TABLE parquet_tbl (id int) USING parquet")
      }.getMessage
      assert(
        error.contains("SparkCatalog can only create paimon table, but current provider is parquet"))
    }
  }
}
