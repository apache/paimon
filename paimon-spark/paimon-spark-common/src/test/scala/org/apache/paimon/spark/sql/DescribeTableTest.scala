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

import java.util.Objects

class DescribeTableTest extends PaimonSparkTestBase {

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
}
