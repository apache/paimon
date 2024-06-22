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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class MergeBranchProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: merge branch test") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a')")
    spark.sql(s"INSERT INTO T VALUES ('2', 'b')")
    spark.sql(s"INSERT INTO T VALUES ('3', 'c')")
    spark.sql(s"INSERT INTO T VALUES ('4', 'd')")

    checkAnswer(
      spark.sql(
        "CALL paimon.sys.create_tag(" +
          "table => 'test.T', tag => 'test_tag', snapshot => 2)"),
      Row(true) :: Nil)

    checkAnswer(
      spark.sql(
        "CALL paimon.sys.create_branch(table => 'test.T', branch => 'test_branch', tag => 'test_tag')"),
      Row(true) :: Nil)

    checkAnswer(
      spark.sql("CALL paimon.sys.merge_branch(table => 'test.T', branch => 'test_branch')"),
      Row(true) :: Nil)

    spark.sql(s"INSERT INTO T VALUES ('5', 'e')")
    spark.sql(s"INSERT INTO T VALUES ('6', 'f')")

    checkAnswer(
      spark.sql("SELECT * FROM T"),
      Row("1", "a") :: Row("2", "b") :: Row("5", "e") :: Row("6", "f") :: Nil)

    checkAnswer(
      spark.sql("CALL paimon.sys.merge_branch(table => 'test.T', branch => 'test_branch')"),
      Row(true) :: Nil)

    // merge_branch again
    checkAnswer(spark.sql("SELECT * FROM T"), Row("1", "a") :: Row("2", "b") :: Nil)

  }

}
