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

class PaimonSystemTableTest extends PaimonSparkTestBase {

  test("system table: sort tags table") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES(1, 'a')")

    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-02')")
    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-01')")
    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-04')")
    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-03')")

    checkAnswer(
      spark.sql("select tag_name from `T$tags`"),
      Row("2024-10-01") :: Row("2024-10-02") :: Row("2024-10-03") :: Row("2024-10-04") :: Nil)
  }

  test("system table: sort partitions table") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING,dt STRING,hh STRING)
                 |PARTITIONED BY (dt, hh)
                 |TBLPROPERTIES ('primary-key'='a,dt,hh', 'bucket' = '3')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES(1, 'a', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(3, 'c', '2024-10-10', '23')")
    spark.sql("INSERT INTO T VALUES(2, 'b', '2024-10-10', '12')")
    spark.sql("INSERT INTO T VALUES(5, 'f', '2024-10-09', '02')")
    spark.sql("INSERT INTO T VALUES(4, 'd', '2024-10-09', '01')")

    checkAnswer(spark.sql("select count(*) from `T$partitions`"), Row(5) :: Nil)
    checkAnswer(
      spark.sql("select partition from `T$partitions`"),
      Row("[2024-10-09, 01]") :: Row("[2024-10-09, 02]") :: Row("[2024-10-10, 01]") :: Row(
        "[2024-10-10, 12]") :: Row("[2024-10-10, 23]") :: Nil
    )
  }

  test("system table: sort buckets table") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING,dt STRING,hh STRING)
                 |PARTITIONED BY (dt, hh)
                 |TBLPROPERTIES ('primary-key'='a,dt,hh', 'bucket' = '3')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES(1, 'a', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(2, 'b', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(3, 'c', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(4, 'd', '2024-10-10', '01')")
    spark.sql("INSERT INTO T VALUES(5, 'f', '2024-10-10', '01')")

    checkAnswer(spark.sql("select count(*) from `T$partitions`"), Row(1) :: Nil)
    checkAnswer(
      spark.sql("select partition,bucket from `T$buckets`"),
      Row("[2024-10-10, 01]", 0) :: Row("[2024-10-10, 01]", 1) :: Row("[2024-10-10, 01]", 2) :: Nil)
  }

  test("system table: binlog table") {
    sql("""
          |CREATE TABLE T (a INT, b INT)
          |TBLPROPERTIES ('primary-key'='a', 'changelog-producer' = 'lookup', 'bucket' = '2')
          |""".stripMargin)

    sql("INSERT INTO T VALUES (1, 2)")
    sql("INSERT INTO T VALUES (1, 3)")
    sql("INSERT INTO T VALUES (2, 2)")

    checkAnswer(
      sql("SELECT * FROM `T$binlog`"),
      Seq(Row("+I", Array(1), Array(3)), Row("+I", Array(2), Array(2)))
    )
  }
}
