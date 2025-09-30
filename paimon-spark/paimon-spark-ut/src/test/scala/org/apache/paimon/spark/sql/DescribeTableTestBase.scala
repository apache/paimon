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

import org.apache.paimon.partition.PartitionStatistics
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions

import java.util.Objects

abstract class DescribeTableTestBase extends PaimonSparkTestBase {

  test("Paimon show: show table extended") {
    val testDB = "test_show"
    withTable(s"$dbName0.s1") {
      spark.sql("CREATE TABLE s1 (id INT)")
      withDatabase(testDB) {
        spark.sql(s"CREATE DATABASE $testDB")
        spark.sql(s"USE $testDB")
        withTable("s2", "s3") {
          spark.sql("CREATE TABLE s2 (id INT, pt STRING) PARTITIONED BY (pt)")
          spark.sql("CREATE TABLE s3 (id INT, pt1 STRING, pt2 STRING) PARTITIONED BY (pt1, pt2)")

          spark.sql("INSERT INTO s2 VALUES (1, '2024'), (2, '2024'), (3, '2025'), (4, '2026')")
          spark.sql("""
                      |INSERT INTO s3
                      |VALUES
                      |(1, '2024', '11'), (2, '2024', '12'), (3, '2025', '11'), (4, '2025', '12')
                      |""".stripMargin)

          // SHOW TABLE EXTENDED will give four columns: namespace, tableName, isTemporary, information.
          checkAnswer(
            sql(s"SHOW TABLE EXTENDED IN $dbName0 LIKE '*'")
              .select("namespace", "tableName", "isTemporary"),
            Row("test", "s1", false))
          checkAnswer(
            sql(s"SHOW TABLE EXTENDED IN $testDB LIKE '*'")
              .select("namespace", "tableName", "isTemporary"),
            Row(testDB, "s2", false) :: Row(testDB, "s3", false) :: Nil
          )

          // check table s1
          val res1 = spark.sql(s"SHOW TABLE EXTENDED IN $testDB LIKE 's2'").select("information")
          Assertions.assertEquals(1, res1.count())
          val information1 = res1
            .collect()
            .head
            .getString(0)
            .split("\n")
            .map {
              line =>
                val kv = line.split(": ", 2)
                kv(0) -> kv(1)
            }
            .toMap
          Assertions.assertEquals(information1("Catalog"), "paimon")
          Assertions.assertEquals(information1("Namespace"), testDB)
          Assertions.assertEquals(information1("Table"), "s2")
          Assertions.assertEquals(information1("Provider"), "paimon")
          Assertions.assertEquals(
            information1("Location"),
            loadTable(testDB, "s2").location().toString)

          // check table s2 partition info
          val error1 = intercept[Exception] {
            spark.sql(s"SHOW TABLE EXTENDED IN $testDB LIKE 's2' PARTITION(pt='2022')")
          }.getMessage
          assert(error1.contains("PARTITIONS_NOT_FOUND"))

          val error2 = intercept[Exception] {
            spark.sql(s"SHOW TABLE EXTENDED IN $testDB LIKE 's3' PARTITION(pt1='2024')")
          }.getMessage
          assert(error2.contains("Partition spec is invalid"))

          val res2 =
            spark.sql(s"SHOW TABLE EXTENDED IN $testDB LIKE 's3' PARTITION(pt1 = '2024', pt2 = 11)")
          checkAnswer(
            res2.select("namespace", "tableName", "isTemporary"),
            Row(testDB, "s3", false)
          )
          Assertions.assertTrue(
            res2.select("information").collect().head.getString(0).contains("Partition Values"))
        }
      }
    }
  }

  test(s"Paimon describe: describe table comment") {
    var comment = "test comment"
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  id INT COMMENT 'id comment',
                 |  name STRING,
                 |  dt STRING)
                 |COMMENT '$comment'
                 |""".stripMargin)
    checkTableCommentEqual("T", comment)

    comment = "new comment"
    spark.sql(s"ALTER TABLE T SET TBLPROPERTIES ('comment' = '$comment')")
    checkTableCommentEqual("T", comment)

    comment = "  "
    spark.sql(s"ALTER TABLE T SET TBLPROPERTIES ('comment' = '$comment')")
    checkTableCommentEqual("T", comment)

    comment = ""
    spark.sql(s"ALTER TABLE T SET TBLPROPERTIES ('comment' = '$comment')")
    checkTableCommentEqual("T", comment)

    spark.sql(s"ALTER TABLE T UNSET TBLPROPERTIES ('comment')")
    checkTableCommentEqual("T", null)

    comment = "new comment"
    spark.sql(s"ALTER TABLE T SET TBLPROPERTIES ('comment' = '$comment')")
    checkTableCommentEqual("T", comment)
  }

  test(s"Paimon describe: describe table with no comment") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  id INT COMMENT 'id comment',
                 |  name STRING,
                 |  dt STRING)
                 |""".stripMargin)
    checkTableCommentEqual("T", null)
  }

  def checkTableCommentEqual(tableName: String, comment: String): Unit = {
    // check describe table
    checkAnswer(
      spark
        .sql(s"DESCRIBE TABLE EXTENDED $tableName")
        .filter("col_name = 'Comment'")
        .select("col_name", "data_type"),
      if (comment == null) Nil else Row("Comment", comment) :: Nil
    )

    // check comment in schema
    Assertions.assertTrue(Objects.equals(comment, loadTable(tableName).schema().comment()))
  }

  test("Paimon describe: describe table partition") {
    spark.sql("""
                |CREATE TABLE T (
                |  id INT NOT NULL,
                |  name STRING DEFAULT 'Bob',
                |  age INT COMMENT 'Age')
                |  PARTITIONED BY (year INT COMMENT 'Year', month INT, day INT)
                |""".stripMargin)
    spark.sql("""
                |INSERT INTO T VALUES (1, 'a', 16, 2025, 9, 20),
                |                     (2, 'b', 17, 2025, 9, 21),
                |                     (3, 'c', 18, 2025, 9, 21)
                |""".stripMargin)
    val res = spark
      .sql("""
             |DESCRIBE FORMATTED T PARTITION (year=2025, month=9, day=21)
             |""".stripMargin)
    checkAnswer(
      res
        .filter("col_name = 'Partition Values'")
        .select("col_name", "data_type"),
      Row("Partition Values", "[year=2025, month=9, day=21]") :: Nil
    )
    // Check comment.
    checkAnswer(
      res
        .filter("col_name = 'age' AND comment != ''")
        .select("col_name", "data_type", "comment"),
      Row("age", "int", "Age") :: Nil
    )
    // Check default value.
    checkAnswer(
      res
        .filter("col_name = 'name' AND comment != ''")
        .select("col_name", "data_type", "comment"),
      Row("name", "string", "'Bob'") :: Nil
    )
    // Check column not null.
    checkAnswer(
      res
        .filter("col_name = 'id' AND comment != ''")
        .select("col_name", "data_type", "comment"),
      Row("id", "int", "NOT NULL") :: Nil
    )
    // Check partition parameters.
    val parameters = res
      .filter("col_name = 'Partition Parameters'")
      .select("data_type")
      .collect()
      .map(_.getString(0))
    Assertions.assertEquals(parameters.length, 1)
    Assertions.assertTrue(parameters.head.contains(PartitionStatistics.FIELD_FILE_COUNT))
    Assertions.assertTrue(parameters.head.contains(PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES))
    Assertions.assertTrue(
      parameters.head.contains(PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME))
    Assertions.assertTrue(parameters.head.contains(PartitionStatistics.FIELD_RECORD_COUNT))
  }
}
