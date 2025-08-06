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

class PartitionExpireTest extends PaimonSparkTestBase {

  test("Partition Expire: base test") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, v INT, dt STRING)
            |TBLPROPERTIES (
            |'partition.expiration-time' = '7d',
            |'end-input.check-partition-expire' = 'true'
            |)
            |PARTITIONED BY (dt)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 1, '2025-06-01')")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq()
      )

      sql("INSERT INTO t VALUES (2, 2, '9999-12-31')")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(2, 2, "9999-12-31"))
      )

      sql("INSERT INTO t VALUES (3, 3, '2025-06-02')")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(2, 2, "9999-12-31"))
      )
    }
  }

  test("Partition Expire: update time expiration strategy") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, v INT, dt STRING)
            |TBLPROPERTIES (
            |'partition.expiration-time' = '3s',
            |'end-input.check-partition-expire' = 'true',
            |'partition.expiration-strategy' = 'update-time'
            |)
            |PARTITIONED BY (dt)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 1, '2025-06-01')")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 1, "2025-06-01"))
      )

      Thread.sleep(3001)
      sql("INSERT INTO t VALUES (2, 2, '2025-06-02')")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(2, 2, "2025-06-02"))
      )
    }
  }

  test("Partition Expire: timestamp formatter and timestamp pattern") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, v INT, p STRING, dt STRING)
            |TBLPROPERTIES (
            |'partition.expiration-time' = '7d',
            |'end-input.check-partition-expire' = 'true',
            |'partition.timestamp-formatter' = 'yyyyMMdd',
            |'partition.timestamp-pattern' = '$dt'
            |)
            |PARTITIONED BY (p, dt)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 1, 'un-known', '20250601')")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq()
      )

      sql("INSERT INTO t VALUES (2, 2, 'un-known', '99991231')")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(2, 2, "un-known", "99991231"))
      )
    }
  }
}
