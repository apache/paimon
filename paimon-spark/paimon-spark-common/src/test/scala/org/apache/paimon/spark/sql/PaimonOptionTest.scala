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
    withSQLConf("spark.paimon.file.block-size" -> "512M") {
      sql("CREATE TABLE T (id INT)")
      val table = loadTable("T")
      // check options in schema file directly
      val fileStoreTable = FileStoreTableFactory.create(table.fileIO(), table.location())
      Assertions.assertEquals("512M", fileStoreTable.options().get("file.block-size"))
    }
  }

  test("Paimon Option: create table by dataframe with sql conf") {
    withSQLConf("spark.paimon.file.block-size" -> "512M") {
      Seq((1L, "x1"), (2L, "x2"))
        .toDF("a", "b")
        .write
        .format("paimon")
        .mode("append")
        .saveAsTable("T")
      val table = loadTable("T")
      // check options in schema file directly
      val fileStoreTable = FileStoreTableFactory.create(table.fileIO(), table.location())
      Assertions.assertEquals("512M", fileStoreTable.options().get("file.block-size"))
    }
  }

  test("Paimon Option: query table with sql conf") {
    sql("CREATE TABLE T (id INT)")
    sql("INSERT INTO T VALUES 1")
    sql("INSERT INTO T VALUES 2")
    checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1) :: Row(2) :: Nil)
    val table = loadTable("T")

    // query with mutable option
    withSQLConf("spark.paimon.scan.snapshot-id" -> "1") {
      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1))
      checkAnswer(spark.read.format("paimon").load(table.location().toString), Row(1))
    }

    // query with immutable option
    withSQLConf("spark.paimon.bucket" -> "1") {
      assertThrows[UnsupportedOperationException] {
        sql("SELECT * FROM T ORDER BY id")
      }
      assertThrows[UnsupportedOperationException] {
        spark.read.format("paimon").load(table.location().toString)
      }
    }
  }
}
