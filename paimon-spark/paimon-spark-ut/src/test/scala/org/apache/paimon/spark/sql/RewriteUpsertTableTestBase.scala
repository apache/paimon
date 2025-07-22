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

abstract class RewriteUpsertTableTestBase extends PaimonSparkTestBase {

  test("Rewrite Upsert Table: cannot define with primary key") {
    assert(intercept[Exception] {
      sql("""
            |CREATE TABLE T (k INT, a INT, b STRING)
            |TBLPROPERTIES ('upsert-key' = 'k', 'primary-key' = 'k')
            |""".stripMargin)
    }.getMessage.contains("Cannot define 'upsert-key' [k] with 'primary-key' [k]."))
  }

  test("Rewrite Upsert Table: rewrite insert without sequence field") {
    sql("""
          |CREATE TABLE T (k INT, a INT, b STRING)
          |TBLPROPERTIES ('upsert-key' = 'k')
          |""".stripMargin)

    sql("INSERT INTO T values (1, 1, 'c1'), (null, 2, 'c2'), (3, 3, 'c3')")
    sql("INSERT INTO T values (null, 22, 'c22'), (1, 11, 'c11'), (4, 4, 'c4')")

    checkAnswer(
      sql("SELECT * FROM T ORDER BY k"),
      Seq(Row(null, 22, "c22"), Row(1, 11, "c11"), Row(3, 3, "c3"), Row(4, 4, "c4")))
  }

  test("Rewrite Upsert Table: rewrite insert with sequence field") {
    sql("""
          |CREATE TABLE T (k1 INT, k2 INT, ts1 INT, ts2 INT, c STRING)
          |TBLPROPERTIES ('upsert-key' = 'k1,k2', 'sequence.field' = 'ts1,ts2')
          |""".stripMargin)

    // test insert deduplicate
    sql("""
          |INSERT INTO T values
          |(null, null, 2, 1, 'v3'),
          |(null, null, 2, 2, 'v4'),
          |(null, null, 1, 1, 'v1'),
          |(null, null, 1, 2, 'v2'),
          |(1, null, 1, 1, 'v1'),
          |(1, 2, 1, 1, 'v1'),
          |(1, 2, 2, 1, 'v2')
          |""".stripMargin)
    checkAnswer(
      sql("SELECT * FROM T ORDER BY k1, k2"),
      Seq(Row(null, null, 2, 2, "v4"), Row(1, null, 1, 1, "v1"), Row(1, 2, 2, 1, "v2")))

    // test inset with different sequence field
    sql("""
          |INSERT INTO T values
          |(null, null, 2, 1, 'v44'),
          |(1, null, 2, 1, 'v2'),
          |(null, 1, 1, 1, 'v1'),
          |(1, 2, 2, 2, 'v3'),
          |(1, 1, 1, 1, 'v1')
          |""".stripMargin)
    checkAnswer(
      sql("SELECT * FROM T ORDER BY k1, k2"),
      Seq(
        Row(null, null, 2, 2, "v4"),
        Row(null, 1, 1, 1, "v1"),
        Row(1, null, 2, 1, "v2"),
        Row(1, 1, 1, 1, "v1"),
        Row(1, 2, 2, 2, "v3")))
  }
}
