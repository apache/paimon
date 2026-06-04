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
                 |CREATE TABLE T (id INT, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('branch-merge.enabled' = 'true')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES (1, 'a')")

    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag1')")

    checkAnswer(
      spark.sql(
        "CALL paimon.sys.create_branch(table => 'test.T', branch => 'test_branch', tag => 'tag1')"),
      Row(true) :: Nil)

    spark.sql(s"INSERT INTO `T$$branch_test_branch` VALUES (2, 'b')")

    checkAnswer(
      spark.sql("CALL paimon.sys.merge_branch(table => 'test.T', source_branch => 'test_branch')"),
      Row(true) :: Nil)

    checkAnswer(spark.sql("SELECT * FROM T ORDER BY id"), Row(1, "a") :: Row(2, "b") :: Nil)
  }

  test("Paimon procedure: merge branch on primary key table should fail") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id', 'bucket'='1')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES (1, 'a')")

    spark.sql("CALL paimon.sys.create_branch(table => 'test.T', branch => 'test_branch')")

    intercept[Exception] {
      spark.sql("CALL paimon.sys.merge_branch(table => 'test.T', source_branch => 'test_branch')")
    }
  }

  test("Paimon procedure: merge branch with non-existent branch should fail") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('branch-merge.enabled' = 'true')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES (1, 'a')")

    intercept[Exception] {
      spark.sql("CALL paimon.sys.merge_branch(table => 'test.T', source_branch => 'nonexistent')")
    }
  }

  test("Paimon procedure: merge branch with explicit target branch") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('branch-merge.enabled' = 'true')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES (1, 'a')")

    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag1')")

    spark.sql(
      "CALL paimon.sys.create_branch(table => 'test.T', branch => 'branchA', tag => 'tag1')")
    spark.sql(
      "CALL paimon.sys.create_branch(table => 'test.T', branch => 'branchB', tag => 'tag1')")

    spark.sql(s"INSERT INTO `T$$branch_branchA` VALUES (2, 'b')")

    checkAnswer(
      spark.sql(
        "CALL paimon.sys.merge_branch(table => 'test.T', source_branch => 'branchA', target_branch => 'branchB')"),
      Row(true) :: Nil)

    checkAnswer(
      spark.sql(s"SELECT * FROM `T$$branch_branchB` ORDER BY id"),
      Row(1, "a") :: Row(2, "b") :: Nil)
  }

}
