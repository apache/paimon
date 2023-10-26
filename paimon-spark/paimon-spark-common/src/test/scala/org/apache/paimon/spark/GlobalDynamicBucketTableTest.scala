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

import org.assertj.core.api.Assertions.assertThat

class GlobalDynamicBucketTableTest extends PaimonSparkTestBase {

  test(s"test global dynamic bucket") {
    spark.sql(s"""
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

    spark.sql("INSERT INTO T VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)")
    val rows1 = spark.sql("SELECT * FROM T ORDER BY pk").collectAsList()
    assertThat(rows1.toString).isEqualTo("[[1,1,1], [1,2,2], [1,3,3], [1,4,4], [1,5,5]]")

    spark.sql("INSERT INTO T VALUES (1, 3, 33), (1, 1, 11)")
    val rows2 = spark.sql("SELECT * FROM T ORDER BY pk").collectAsList()
    assertThat(rows2.toString).isEqualTo("[[1,1,11], [1,2,2], [1,3,33], [1,4,4], [1,5,5]]")

    val rows3 = spark.sql("SELECT DISTINCT bucket FROM `T$FILES`").collectAsList()
    assertThat(rows3.toString).isEqualTo("[[0], [1]]")

    // change partition
    spark.sql("INSERT INTO T VALUES (2, 1, 2), (2, 2, 3)")
    val rows4 = spark.sql("SELECT * FROM T ORDER BY pk").collectAsList()
    assertThat(rows4.toString).isEqualTo("[[2,1,2], [2,2,3], [1,3,33], [1,4,4], [1,5,5]]")
  }

  test(s"test global dynamic bucket with delete") {
    spark.sql(s"""
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

    spark.sql("INSERT INTO T VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)")
    val rows1 = spark.sql("SELECT * FROM T ORDER BY pk").collectAsList()
    assertThat(rows1.toString).isEqualTo("[[1,1,1], [1,2,2], [1,3,3], [1,4,4], [1,5,5]]")

    spark.sql("DELETE FROM T WHERE pk = 1")
    val rows2 = spark.sql("SELECT * FROM T ORDER BY pk").collectAsList()
    assertThat(rows2.toString).isEqualTo("[[1,2,2], [1,3,3], [1,4,4], [1,5,5]]")

    // change partition
    spark.sql("INSERT INTO T VALUES (2, 1, 2), (2, 2, 3)")
    val rows3 = spark.sql("SELECT * FROM T ORDER BY pk").collectAsList()
    assertThat(rows3.toString).isEqualTo("[[2,1,2], [2,2,3], [1,3,3], [1,4,4], [1,5,5]]")

    spark.sql("DELETE FROM T WHERE pk = 2")
    val rows4 = spark.sql("SELECT * FROM T ORDER BY pk").collectAsList()
    assertThat(rows4.toString).isEqualTo("[[2,1,2], [1,3,3], [1,4,4], [1,5,5]]")
  }

  test(s"test write with assigner parallelism") {
    spark.sql(s"""
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

    spark.sql("INSERT INTO T VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)")
    val rows1 = spark.sql("SELECT * FROM T ORDER BY pk").collectAsList()
    assertThat(rows1.toString).isEqualTo("[[1,1,1], [1,2,2], [1,3,3], [1,4,4], [1,5,5]]")

    val rows3 = spark.sql("SELECT DISTINCT bucket FROM `T$FILES`").collectAsList()
    assertThat(rows3.toString).isEqualTo("[[0], [1], [2]]")
  }

}
