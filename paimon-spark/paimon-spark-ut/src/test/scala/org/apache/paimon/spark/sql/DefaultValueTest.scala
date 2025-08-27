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

class DefaultValueTest extends PaimonSparkTestBase {

  test("Default Value: current_timestamp and current_date") {
    withTimeZone("Asia/Shanghai") {
      withTable("t") {
        sql("""
              |CREATE TABLE T (
              |  a INT,
              |  b TIMESTAMP DEFAULT current_timestamp(),
              |  c TIMESTAMP_NTZ DEFAULT current_timestamp,
              |  d DATE DEFAULT current_date()
              |)
              |""".stripMargin)
        sql("INSERT INTO T (a) VALUES (1), (2)")

        checkAnswer(
          sql("SELECT a FROM T WHERE b >= (current_timestamp() - INTERVAL '1' MINUTE)"),
          Seq(Row(1), Row(2))
        )
        checkAnswer(
          sql("SELECT a FROM T WHERE c >= (current_timestamp() - INTERVAL '1' MINUTE)"),
          Seq(Row(1), Row(2))
        )
        checkAnswer(
          sql("SELECT a FROM T WHERE d >= (current_date() - INTERVAL '1' DAY)"),
          Seq(Row(1), Row(2))
        )
      }
    }
  }
}
