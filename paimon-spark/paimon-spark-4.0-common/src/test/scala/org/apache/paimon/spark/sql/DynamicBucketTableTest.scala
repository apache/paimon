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

  test(s"Paimon dynamic bucket table: write with global dynamic bucket") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  pk STRING,
                 |  v STRING,
                 |  pt STRING)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'pk',
                 |  'bucket' = '-1'
                 |)
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    val error = intercept[UnsupportedOperationException] {
      spark.sql("INSERT INTO T VALUES ('1', 'a', 'p')")
    }.getMessage
    assert(error.contains("Spark doesn't support CROSS_PARTITION mode"))
  }
}
