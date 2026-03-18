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

  test("Default Value: MAP TYPE") {
    withTable("t") {
      spark.sql("""CREATE TABLE IF NOT EXISTS t (
                  |  id BIGINT NOT NULL,
                  |  col1 MAP<INT, DOUBLE> DEFAULT NULL,
                  |  col2 MAP<INT, DOUBLE> DEFAULT map(),
                  |  col3 MAP<INT, DOUBLE> DEFAULT map(99, 99.99D)
                  |) using paimon;
                  |""".stripMargin)

      spark.sql("""
                  |INSERT INTO t VALUES
                  |  (1, map(1, 1.1), map(2, 2.2), map(3, 3.3)),
                  |  (2, null, map(5, 5.5), map(6, 6.6)),
                  |  (3, map(7, 7.7), null, map(9, 9.9)),
                  |  (4, map(10, 10.1), map(11, 11.11), null),
                  |  (5, null, null, null)
                  |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Row(1, Map(1 -> 1.1), Map(2 -> 2.2), Map(3 -> 3.3))
          :: Row(2, null, Map(5 -> 5.5), Map(6 -> 6.6))
          :: Row(3, Map(7 -> 7.7), Map(), Map(9 -> 9.9))
          :: Row(4, Map(10 -> 10.1), Map(11 -> 11.11), Map(99 -> 99.99))
          :: Row(5, null, Map(), Map(99 -> 99.99))
          :: Nil
      )
    }
  }

  test("Default Value: map type with string element") {
    withTable("t") {
      spark.sql("""CREATE TABLE IF NOT EXISTS t (
                  |  id BIGINT NOT NULL,
                  |  col1 MAP<BIGINT, STRING> DEFAULT NULL,
                  |  col2 MAP<BIGINT, STRING> DEFAULT map(),
                  |  col3 MAP<BIGINT, STRING> DEFAULT map(9999, "hello")
                  |) using paimon;
                  |""".stripMargin)

      spark.sql("""
                  |INSERT INTO t VALUES
                  |  (1, map(1, "a", 11, "aa"), map(2, "b"), map(3, "c")),
                  |  (2, null, map(5, "e", 55, "ee"), map(6, "f")),
                  |  (3, map(7, "g"), null, map(9, "i", 99, "ii")),
                  |  (4, map(10, "j"), map(11, "k"), null),
                  |  (5, null, null, map(15, "o")),
                  |  (6, null, map(17, "q"), null),
                  |  (7, map(19, "r"), null, null),
                  |  (8, null, null, null)
                  |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Row(1, Map(1 -> "a", 11 -> "aa"), Map(2 -> "b"), Map(3 -> "c"))
          :: Row(2, null, Map(5 -> "e", 55 -> "ee"), Map(6 -> "f"))
          :: Row(3, Map(7 -> "g"), Map(), Map(9 -> "i", 99 -> "ii"))
          :: Row(4, Map(10 -> "j"), Map(11 -> "k"), Map(9999 -> "hello"))
          :: Row(5, null, Map(), Map(15 -> "o"))
          :: Row(6, null, Map(17 -> "q"), Map(9999 -> "hello"))
          :: Row(7, Map(19 -> "r"), Map(), Map(9999 -> "hello"))
          :: Row(8, null, Map(), Map(9999 -> "hello"))
          :: Nil
      )
    }
  }

  test("Default Value: unsupported default value") {
    withTimeZone("Asia/Shanghai") {
      withTable("t") {
        assert(
          intercept[Throwable] {
            sql("""
                  |CREATE TABLE t (
                  |  a INT,
                  |  b TIMESTAMP DEFAULT current_timestamp(),
                  |  c TIMESTAMP_NTZ DEFAULT current_timestamp,
                  |  d DATE DEFAULT current_date()
                  |)
                  |""".stripMargin)
          }.getMessage
            .contains("Unsupported default value `current_timestamp()`"))

        sql("CREATE TABLE t (a INT DEFAULT 1)")
        assert(
          intercept[Throwable] {
            sql("""
                  |ALTER TABLE t ALTER COLUMN a SET DEFAULT current_timestamp()
                  |""".stripMargin)
          }.getMessage
            .contains("Unsupported default value `current_timestamp()`"))
      }
    }
  }
}
