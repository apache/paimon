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

class DynamicBucketTableTest extends PaimonSparkTestBase {

  test(s"Paimon dynamic bucket table: write with assign parallelism") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  pk STRING,
                 |  v STRING,
                 |  pt STRING)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'pk, pt',
                 |  'bucket' = '-1',
                 |  'dynamic-bucket.target-row-num'='3',
                 |  'dynamic-bucket.assigner-parallelism'='3'
                 |)
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    spark.sql(
      "INSERT INTO T VALUES ('1', 'a', 'p'), ('2', 'b', 'p'), ('3', 'c', 'p'), ('4', 'd', 'p'), ('5', 'e', 'p')")

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY pk"),
      Row("1", "a", "p") :: Row("2", "b", "p") :: Row("3", "c", "p") :: Row("4", "d", "p") :: Row(
        "5",
        "e",
        "p") :: Nil)

    checkAnswer(
      spark.sql("SELECT DISTINCT bucket FROM `T$FILES`"),
      Row(0) :: Row(1) :: Row(2) :: Nil)
  }

  test(s"Paimon dynamic bucket table: write with dynamic-bucket.initial-buckets") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  pk STRING,
                 |  v STRING,
                 |  pt STRING)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'pk, pt',
                 |  'bucket' = '-1',
                 |  'dynamic-bucket.target-row-num'='3',
                 |  'dynamic-bucket.initial-buckets'='3',
                 |  'dynamic-bucket.assigner-parallelism'='10'
                 |)
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    spark.sql(
      "INSERT INTO T VALUES ('1', 'a', 'p'), ('2', 'b', 'p'), ('3', 'c', 'p'), ('4', 'd', 'p'), ('5', 'e', 'p')")

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY pk"),
      Row("1", "a", "p") :: Row("2", "b", "p") :: Row("3", "c", "p") :: Row("4", "d", "p") :: Row(
        "5",
        "e",
        "p") :: Nil)

    // Although `assigner-parallelism` is 10, but because `initial-buckets` is 3, the size of generated buckets is still 3
    checkAnswer(
      spark.sql("SELECT DISTINCT bucket FROM `T$FILES`"),
      Row(0) :: Row(1) :: Row(2) :: Nil)
  }

  test(s"Paimon cross partition table: write with partition change") {
    sql(s"""
           |CREATE TABLE T (
           |  pt INT,
           |  pk INT,
           |  v INT)
           |TBLPROPERTIES (
           |  'primary-key' = 'pk',
           |  'bucket' = '-1',
           |  'dynamic-bucket.target-row-num'='3',
           |  'dynamic-bucket.assigner-parallelism'='1'
           |)
           |PARTITIONED BY (pt)
           |""".stripMargin)

    sql("INSERT INTO T VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)")
    checkAnswer(
      sql("SELECT * FROM T ORDER BY pk"),
      Seq(Row(1, 1, 1), Row(1, 2, 2), Row(1, 3, 3), Row(1, 4, 4), Row(1, 5, 5)))

    sql("INSERT INTO T VALUES (1, 3, 33), (1, 1, 11)")
    checkAnswer(
      sql("SELECT * FROM T ORDER BY pk"),
      Seq(Row(1, 1, 11), Row(1, 2, 2), Row(1, 3, 33), Row(1, 4, 4), Row(1, 5, 5)))

    checkAnswer(sql("SELECT DISTINCT bucket FROM `T$FILES`"), Seq(Row(0), Row(1)))

    // change partition
    sql("INSERT INTO T VALUES (2, 1, 2), (2, 2, 3)")
    checkAnswer(
      sql("SELECT * FROM T ORDER BY pk"),
      Seq(Row(2, 1, 2), Row(2, 2, 3), Row(1, 3, 33), Row(1, 4, 4), Row(1, 5, 5)))
  }

  test(s"Paimon cross partition table: write with delete") {
    sql(s"""
           |CREATE TABLE T (
           |  pt INT,
           |  pk INT,
           |  v INT)
           |TBLPROPERTIES (
           |  'primary-key' = 'pk',
           |  'bucket' = '-1',
           |  'dynamic-bucket.target-row-num'='3'
           |)
           |PARTITIONED BY (pt)
           |""".stripMargin)

    sql("INSERT INTO T VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)")
    checkAnswer(
      sql("SELECT * FROM T ORDER BY pk"),
      Seq(Row(1, 1, 1), Row(1, 2, 2), Row(1, 3, 3), Row(1, 4, 4), Row(1, 5, 5)))

    sql("DELETE FROM T WHERE pk = 1")
    checkAnswer(
      sql("SELECT * FROM T ORDER BY pk"),
      Seq(Row(1, 2, 2), Row(1, 3, 3), Row(1, 4, 4), Row(1, 5, 5)))

    // change partition
    sql("INSERT INTO T VALUES (2, 1, 2), (2, 2, 3)")
    checkAnswer(
      sql("SELECT * FROM T ORDER BY pk"),
      Seq(Row(2, 1, 2), Row(2, 2, 3), Row(1, 3, 3), Row(1, 4, 4), Row(1, 5, 5)))

    sql("DELETE FROM T WHERE pk = 2")
    checkAnswer(
      sql("SELECT * FROM T ORDER BY pk"),
      Seq(Row(2, 1, 2), Row(1, 3, 3), Row(1, 4, 4), Row(1, 5, 5)))
  }

  test(s"Paimon cross partition table: user define assigner parallelism") {
    sql(s"""
           |CREATE TABLE T (
           |  pt INT,
           |  pk INT,
           |  v INT)
           |TBLPROPERTIES (
           |  'primary-key' = 'pk',
           |  'bucket' = '-1',
           |  'dynamic-bucket.target-row-num'='3',
           |  'dynamic-bucket.assigner-parallelism'='3'
           |)
           |PARTITIONED BY (pt)
           |""".stripMargin)

    sql("INSERT INTO T VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)")
    checkAnswer(
      sql("SELECT * FROM T ORDER BY pk"),
      Seq(Row(1, 1, 1), Row(1, 2, 2), Row(1, 3, 3), Row(1, 4, 4), Row(1, 5, 5)))

    checkAnswer(sql("SELECT DISTINCT bucket FROM `T$FILES`"), Seq(Row(0), Row(1), Row(2)))
  }
}
