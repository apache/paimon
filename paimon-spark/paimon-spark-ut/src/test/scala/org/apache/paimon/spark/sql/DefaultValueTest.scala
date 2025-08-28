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

class DefaultValueTest extends PaimonSparkTestBase {

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
