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

import org.apache.paimon.spark.{PaimonScan, PaimonSparkTestBase}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

abstract class MapSelectedKeysSharedShreddingE2ETestBase extends PaimonSparkTestBase {

  Seq("parquet", "orc").foreach {
    format =>
      test(s"read selected shared-shredding map keys directly from $format") {
        withTable("T") {
          sql(s"""
                 |CREATE TABLE T (id INT, attrs MAP<STRING, BIGINT>)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'file.format' = '$format',
                 |  'fields.attrs.map.storage-layout' = 'shared-shredding',
                 |  'fields.attrs.map.shared-shredding.max-columns' = '1'
                 |)
                 |""".stripMargin)

          sql("""
                |INSERT INTO T VALUES
                |  (1, map('key1', CAST(10 AS BIGINT), 'key2', CAST(20 AS BIGINT))),
                |  (2, map('key2', CAST(30 AS BIGINT), 'cold', CAST(40 AS BIGINT)))
                |""".stripMargin)

          val query =
            sql("SELECT id, attrs['key1'], attrs['key2'], attrs['missing'] FROM T ORDER BY id")
          val sparkPlan = query.queryExecution.sparkPlan
          val pushedMapSelectedKeys = sparkPlan.collectFirst {
            case scan: BatchScanExec if scan.scan.isInstanceOf[PaimonScan] =>
              scan.scan.asInstanceOf[PaimonScan].pushedMapSelectedKeys
          }
          val expectedPushedMapSelectedKeys = Map("attrs" -> Seq("key1", "key2", "missing"))
          assert(
            pushedMapSelectedKeys.contains(expectedPushedMapSelectedKeys),
            s"""Expected selected MAP keys to be pushed down.
               |Physical plan:
               |$sparkPlan""".stripMargin
          )

          checkAnswer(query, Row(1, 10L, 20L, null) :: Row(2, null, 30L, null) :: Nil)
        }
      }

      test(s"read full shared-shredding map column from $format") {
        withTable("T") {
          sql(s"""
                 |CREATE TABLE T (id INT, attrs MAP<STRING, BIGINT>)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'file.format' = '$format',
                 |  'fields.attrs.map.storage-layout' = 'shared-shredding',
                 |  'fields.attrs.map.shared-shredding.max-columns' = '1'
                 |)
                 |""".stripMargin)

          sql("""
                |INSERT INTO T VALUES
                |  (1, map('key1', CAST(10 AS BIGINT), 'key2', CAST(20 AS BIGINT))),
                |  (2, map('key2', CAST(30 AS BIGINT), 'cold', CAST(40 AS BIGINT))),
                |  (3, NULL)
                |""".stripMargin)

          checkAnswer(
            sql("SELECT id, attrs FROM T ORDER BY id"),
            Row(1, Map("key1" -> 10L, "key2" -> 20L)) ::
              Row(2, Map("key2" -> 30L, "cold" -> 40L)) ::
              Row(3, null) :: Nil)
        }
      }

      test(s"read selected normal map keys from $format") {
        withTable("T") {
          sql(s"""
                 |CREATE TABLE T (id INT, attrs MAP<STRING, BIGINT>)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'file.format' = '$format'
                 |)
                 |""".stripMargin)

          sql("""
                |INSERT INTO T VALUES
                |  (1, map('key1', CAST(10 AS BIGINT), 'key2', CAST(20 AS BIGINT))),
                |  (2, map('key2', CAST(30 AS BIGINT), 'cold', CAST(40 AS BIGINT))),
                |  (3, NULL)
                |""".stripMargin)

          checkAnswer(
            sql("SELECT id, attrs['key1'], attrs['key2'], attrs['missing'] FROM T ORDER BY id"),
            Row(1, 10L, 20L, null) ::
              Row(2, null, 30L, null) ::
              Row(3, null, null, null) :: Nil
          )
        }
      }

      test(s"read full normal map column from $format") {
        withTable("T") {
          sql(s"""
                 |CREATE TABLE T (id INT, attrs MAP<STRING, BIGINT>)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'file.format' = '$format'
                 |)
                 |""".stripMargin)

          sql("""
                |INSERT INTO T VALUES
                |  (1, map('key1', CAST(10 AS BIGINT), 'key2', CAST(20 AS BIGINT))),
                |  (2, map('key2', CAST(30 AS BIGINT), 'cold', CAST(40 AS BIGINT))),
                |  (3, NULL)
                |""".stripMargin)

          checkAnswer(
            sql("SELECT id, attrs FROM T ORDER BY id"),
            Row(1, Map("key1" -> 10L, "key2" -> 20L)) ::
              Row(2, Map("key2" -> 30L, "cold" -> 40L)) ::
              Row(3, null) :: Nil)
        }
      }
  }
}
