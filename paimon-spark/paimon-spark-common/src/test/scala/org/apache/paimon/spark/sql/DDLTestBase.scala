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

import org.apache.spark.sql.Row
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
        error.contains(
          "SparkCatalog can only create paimon table, but current provider is parquet"))
    }
  }

  test("Paimon DDL: create table with char/varchar/string") {
    Seq("orc", "avro").foreach(
      format => {
        withTable("paimon_tbl") {
          spark.sql(
            s"""
               |CREATE TABLE paimon_tbl (id int, col_s1 char(9), col_s2 varchar(10), col_s3 string)
               |USING PAIMON
               |TBLPROPERTIES ('file.format' = '$format')
               |""".stripMargin)

          spark.sql(s"""
                       |insert into paimon_tbl values
                       |(1, 'Wednesday', 'Wednesday', 'Wednesday'),
                       |(2, 'Friday', 'Friday', 'Friday')
                       |""".stripMargin)

          // check description
          checkAnswer(
            spark
              .sql(s"DESC paimon_tbl")
              .select("col_name", "data_type")
              .where("col_name LIKE 'col_%'")
              .orderBy("col_name"),
            Row("col_s1", "char(9)") :: Row("col_s2", "varchar(10)") :: Row(
              "col_s3",
              "string") :: Nil
          )

          // check select
          if (format == "orc" && !gteqSpark3_4) {
            // Orc reader will right trim the char type, e.g. "Friday   " => "Friday" (see orc's `CharTreeReader`)
            // and Spark has a conf `spark.sql.readSideCharPadding` to auto padding char only since 3.4 (default true)
            // So when using orc with Spark3.4-, here will return "Friday"
            checkAnswer(
              spark.sql(s"select col_s1 from paimon_tbl where id = 2"),
              Row("Friday") :: Nil
            )
            // Spark will auto create the filter like Filter(isnotnull(col_s1#124) AND (col_s1#124 = Friday   ))
            // for char type, so here will not return any rows
            checkAnswer(
              spark.sql(s"select col_s1 from paimon_tbl where col_s1 = 'Friday'"),
              Nil
            )
          } else {
            checkAnswer(
              spark.sql(s"select col_s1 from paimon_tbl where id = 2"),
              Row("Friday   ") :: Nil
            )
            checkAnswer(
              spark.sql(s"select col_s1 from paimon_tbl where col_s1 = 'Friday'"),
              Row("Friday   ") :: Nil
            )
          }
          checkAnswer(
            spark.sql(s"select col_s2 from paimon_tbl where col_s2 = 'Friday'"),
            Row("Friday") :: Nil
          )
          checkAnswer(
            spark.sql(s"select col_s3 from paimon_tbl where col_s3 = 'Friday'"),
            Row("Friday") :: Nil
          )
        }
      })
  }
}
