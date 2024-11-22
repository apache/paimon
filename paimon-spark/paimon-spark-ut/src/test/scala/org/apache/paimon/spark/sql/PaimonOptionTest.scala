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
import org.apache.paimon.table.FileStoreTableFactory

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions

class PaimonOptionTest extends PaimonSparkTestBase {

  import testImplicits._

  test("Paimon Option: create table with sql conf") {
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "2") {
      sql("CREATE TABLE T (id INT)")
      val table = loadTable("T")
      // check options in schema file directly
      val fileStoreTable = FileStoreTableFactory.create(table.fileIO(), table.location())
      Assertions.assertNull(fileStoreTable.options().get("scan.snapshot-id"))
    }
  }

  test("Paimon Option: create table by dataframe with sql conf") {
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "2") {
      Seq((1L, "x1"), (2L, "x2"))
        .toDF("a", "b")
        .write
        .format("paimon")
        .mode("append")
        .saveAsTable("T")
      val table = loadTable("T")
      // check options in schema file directly
      val fileStoreTable = FileStoreTableFactory.create(table.fileIO(), table.location())
      Assertions.assertNull(fileStoreTable.options().get("scan.snapshot-id"))
    }
  }

  test("Paimon Option: query table with sql conf") {
    sql("CREATE TABLE T (id INT)")
    sql("INSERT INTO T VALUES 1")
    sql("INSERT INTO T VALUES 2")
    checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1) :: Row(2) :: Nil)
    val table = loadTable("T")

    // query with mutable option
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1))
      checkAnswer(spark.read.format("paimon").load(table.location().toString), Row(1))
    }

    // query with immutable option
    withSparkSQLConf("spark.paimon.bucket" -> "1") {
      assertThrows[UnsupportedOperationException] {
        sql("SELECT * FROM T ORDER BY id")
      }
      assertThrows[UnsupportedOperationException] {
        spark.read.format("paimon").load(table.location().toString)
      }
    }
  }

  test("Paimon Table Options: query one table with sql conf and table options") {
    sql("CREATE TABLE T (id INT)")
    sql("INSERT INTO T VALUES 1")
    sql("INSERT INTO T VALUES 2")
    checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1) :: Row(2) :: Nil)
    val table = loadTable("T")

    // query with global options
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1))
      checkAnswer(spark.read.format("paimon").load(table.location().toString), Row(1))
    }

    // query with table options
    withSparkSQLConf("spark.paimon.*.*.T.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1))
      checkAnswer(spark.read.format("paimon").load(table.location().toString), Row(1))
    }

    // query with both global and table options
    withSparkSQLConf(
      "spark.paimon.scan.snapshot-id" -> "1",
      "spark.paimon.*.*.T.scan.snapshot-id" -> "2") {
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1) :: Row(2) :: Nil)
      checkAnswer(
        spark.read.format("paimon").load(table.location().toString),
        Row(1) :: Row(2) :: Nil)
    }
  }

  test("Paimon Table Options: query multiple tables with sql conf and table options") {
    sql("CREATE TABLE T1 (id INT)")
    sql("INSERT INTO T1 VALUES 1")
    sql("INSERT INTO T1 VALUES 2")

    sql("CREATE TABLE T2 (id INT)")
    sql("INSERT INTO T2 VALUES 1")
    sql("INSERT INTO T2 VALUES 2")
    checkAnswer(
      sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"),
      Row(1, 1) :: Row(2, 2) :: Nil)
    val table1 = loadTable("T1")
    val table2 = loadTable("T1")

    // query with global options
    withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"), Row(1, 1))
      checkAnswer(
        spark.read
          .format("paimon")
          .load(table1.location().toString)
          .join(spark.read.format("paimon").load(table2.location().toString), "id"),
        Row(1)
      )
    }

    // query with table options
    withSparkSQLConf("spark.paimon.*.*.*.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"), Row(1, 1))
      checkAnswer(
        spark.read
          .format("paimon")
          .load(table1.location().toString)
          .join(spark.read.format("paimon").load(table2.location().toString), "id"),
        Row(1)
      )
    }

    // query with both global and table options
    withSparkSQLConf(
      "spark.paimon.scan.snapshot-id" -> "1",
      "spark.paimon.*.*.*.scan.snapshot-id" -> "2") {
      checkAnswer(
        sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"),
        Row(1, 1) :: Row(2, 2) :: Nil)
      checkAnswer(
        spark.read
          .format("paimon")
          .load(table1.location().toString)
          .join(spark.read.format("paimon").load(table2.location().toString), "id"),
        Row(1) :: Row(2) :: Nil
      )
    }

    withSparkSQLConf(
      "spark.paimon.*.*.T1.scan.snapshot-id" -> "1",
      "spark.paimon.*.*.T2.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"), Row(1, 1))
      checkAnswer(
        spark.read
          .format("paimon")
          .load(table1.location().toString)
          .join(spark.read.format("paimon").load(table2.location().toString), "id"),
        Row(1)
      )
    }

    withSparkSQLConf(
      "spark.paimon.*.*.T1.scan.snapshot-id" -> "1",
      "spark.paimon.*.*.T2.scan.snapshot-id" -> "2") {
      checkAnswer(sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"), Row(1, 1))
      checkAnswer(
        spark.read
          .format("paimon")
          .load(table1.location().toString)
          .join(spark.read.format("paimon").load(table2.location().toString), "id"),
        Row(1)
      )
    }

    withSparkSQLConf(
      "spark.paimon.*.*.T1.scan.snapshot-id" -> "2",
      "spark.paimon.*.*.T2.scan.snapshot-id" -> "2") {
      checkAnswer(
        sql("SELECT * FROM T1 join T2 on T1.id = T2.id ORDER BY T1.id"),
        Row(1, 1) :: Row(2, 2) :: Nil)
      checkAnswer(
        spark.read
          .format("paimon")
          .load(table1.location().toString)
          .join(spark.read.format("paimon").load(table2.location().toString), "id"),
        Row(1) :: Row(2) :: Nil
      )
    }
  }
}
