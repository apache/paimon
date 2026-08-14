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
import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand

class PaimonDynamicPartitionColumnOrderTest extends PaimonSparkTestBase {

  private val targetTableName = "dynamic_partition_order"

  private def withDynamicPartitionTable(f: => Unit): Unit = {
    withTable(targetTableName) {
      sql(s"""
             |CREATE TABLE $targetTableName (
             |  ds STRING,
             |  part STRING,
             |  uid STRING,
             |  value STRING
             |) PARTITIONED BY (ds, part)
             |""".stripMargin)
      f
    }
  }

  private def analyzedWriteQuery(insert: String) = {
    val parsed = spark.sessionState.sqlParser.parsePlan(insert)
    spark.sessionState.analyzer
      .executeAndCheck(parsed, new QueryPlanningTracker)
      .asInstanceOf[V2WriteCommand]
      .query
  }

  private def tableOrderUnion: String =
    s"""
       |INSERT OVERWRITE $targetTableName PARTITION (ds, part)
       |SELECT '2026-08-10' AS ds, 'p1' AS part, 'u1' AS uid, 'v1' AS detail_ratio
       |UNION ALL
       |SELECT '2026-08-10' AS ds, 'p2' AS part, 'u2' AS uid, 'v2' AS value
       |""".stripMargin

  test("table order preserves UNION output aliases") {
    withSparkSQLConf(
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
      "spark.paimon.write.use-v2-write" -> "true",
      "spark.paimon.sql.dynamic-partition-column-order" -> "table"
    ) {
      withDynamicPartitionTable {
        assert(
          createDataset(spark, analyzedWriteQuery(tableOrderUnion)).collect().toSeq == Seq(
            Row("2026-08-10", "p1", "u1", "v1"),
            Row("2026-08-10", "p2", "u2", "v2")))
      }
    }
  }

  test("community defaults dynamic partition writes to auto order") {
    withSparkSQLConf(
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
      "spark.paimon.write.use-v2-write" -> "true") {
      withDynamicPartitionTable {
        assert(
          createDataset(spark, analyzedWriteQuery(tableOrderUnion)).collect().toSeq == Seq(
            Row("2026-08-10", "p1", "u1", "v1"),
            Row("2026-08-10", "p2", "u2", "v2")))
      }
    }
  }

  test("auto detects table order despite UNION output aliases") {
    withSparkSQLConf(
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
      "spark.paimon.write.use-v2-write" -> "true",
      "spark.paimon.sql.dynamic-partition-column-order" -> "auto"
    ) {
      withDynamicPartitionTable {
        assert(
          createDataset(spark, analyzedWriteQuery(tableOrderUnion)).collect().toSeq == Seq(
            Row("2026-08-10", "p1", "u1", "v1"),
            Row("2026-08-10", "p2", "u2", "v2")))
      }
    }
  }

  test("auto maps Hive order dynamic partition columns to table order") {
    withSparkSQLConf(
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
      "spark.paimon.write.use-v2-write" -> "true",
      "spark.paimon.sql.dynamic-partition-column-order" -> "auto"
    ) {
      withDynamicPartitionTable {
        val hiveOrderInsert =
          s"""
             |INSERT OVERWRITE $targetTableName PARTITION (ds, part)
             |SELECT 'u1' AS uid, 'v1' AS value, '2026-08-10' AS ds, 'p1' AS part
             |""".stripMargin

        assert(
          createDataset(spark, analyzedWriteQuery(hiveOrderInsert)).collect().toSeq == Seq(
            Row("2026-08-10", "p1", "u1", "v1")))
      }
    }
  }

  test("hive order maps dynamic partition columns at the end to table order") {
    Seq("true", "false").foreach {
      useV2Write =>
        withSparkSQLConf(
          "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
          "spark.paimon.write.use-v2-write" -> useV2Write,
          "spark.paimon.sql.dynamic-partition-column-order" -> "hive"
        ) {
          withDynamicPartitionTable {
            sql(s"""
                   |INSERT OVERWRITE $targetTableName PARTITION (ds, part)
                   |SELECT 'u1' AS uid, 'v1' AS value, '2026-08-10' AS ds, 'p1' AS part
                   |""".stripMargin)

            checkAnswer(
              sql(s"SELECT ds, part, uid, value FROM $targetTableName"),
              Row("2026-08-10", "p1", "u1", "v1"))
          }
        }
    }
  }

  test("hive order preserves input already matching table schema") {
    withSparkSQLConf(
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
      "spark.paimon.write.use-v2-write" -> "true",
      "spark.paimon.sql.dynamic-partition-column-order" -> "hive"
    ) {
      withDynamicPartitionTable {
        val tableOrderInsert =
          s"""
             |INSERT OVERWRITE $targetTableName PARTITION (ds, part)
             |SELECT '2026-08-10' AS ds, 'p1' AS part, 'u1' AS uid, 'v1' AS value
             |""".stripMargin

        assert(
          createDataset(spark, analyzedWriteQuery(tableOrderInsert)).collect().toSeq == Seq(
            Row("2026-08-10", "p1", "u1", "v1")))
      }
    }
  }

  test("hive order keeps VALUES positional without partition clause") {
    withSparkSQLConf(
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
      "spark.paimon.write.use-v2-write" -> "true",
      "spark.paimon.sql.dynamic-partition-column-order" -> "hive"
    ) {
      withDynamicPartitionTable {
        sql(s"""
               |INSERT OVERWRITE $targetTableName VALUES
               |  ('2026-08-10', 'p1', 'u1', 'v1')
               |""".stripMargin)

        checkAnswer(
          sql(s"SELECT ds, part, uid, value FROM $targetTableName"),
          Row("2026-08-10", "p1", "u1", "v1"))
      }
    }
  }

  test("auto and hive orders detect named Hive-style output without partition clause") {
    Seq("auto", "hive").foreach {
      columnOrder =>
        withSparkSQLConf(
          "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
          "spark.paimon.write.use-v2-write" -> "true",
          "spark.paimon.sql.dynamic-partition-column-order" -> columnOrder
        ) {
          withDynamicPartitionTable {
            sql(s"""
                   |INSERT OVERWRITE $targetTableName
                   |SELECT 'u1' AS uid, 'v1' AS value, '2026-08-10' AS ds, 'p1' AS part
                   |""".stripMargin)

            checkAnswer(
              sql(s"SELECT ds, part, uid, value FROM $targetTableName"),
              Row("2026-08-10", "p1", "u1", "v1"))
          }
        }
    }
  }

  test("table order remains positional without partition clause") {
    withSparkSQLConf(
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
      "spark.paimon.write.use-v2-write" -> "true",
      "spark.paimon.sql.dynamic-partition-column-order" -> "table"
    ) {
      withDynamicPartitionTable {
        sql(s"""
               |INSERT OVERWRITE $targetTableName
               |SELECT 'u1' AS uid, 'v1' AS value, '2026-08-10' AS ds, 'p1' AS part
               |""".stripMargin)

        checkAnswer(
          sql(s"SELECT ds, part, uid, value FROM $targetTableName"),
          Row("u1", "v1", "2026-08-10", "p1"))
      }
    }
  }

  test("auto and hive orders preserve mixed static and dynamic partitions") {
    Seq("auto", "hive").foreach {
      columnOrder =>
        withSparkSQLConf(
          "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
          "spark.paimon.write.use-v2-write" -> "true",
          "spark.paimon.sql.dynamic-partition-column-order" -> columnOrder
        ) {
          withTable("mixed_partition_order") {
            sql("""
                  |CREATE TABLE mixed_partition_order (
                  |  uid STRING,
                  |  ds STRING,
                  |  value STRING,
                  |  region STRING
                  |) PARTITIONED BY (region, ds)
                  |""".stripMargin)

            sql("""
                  |INSERT OVERWRITE mixed_partition_order PARTITION (region = 'cn', ds)
                  |SELECT 'u1' AS uid, 'v1' AS value, '2026-08-10' AS ds
                  |""".stripMargin)

            checkAnswer(
              sql("SELECT uid, ds, value, region FROM mixed_partition_order"),
              Row("u1", "2026-08-10", "v1", "cn"))
          }
        }
    }
  }

  test("table order remains positional for Hive-style input") {
    withSparkSQLConf(
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
      "spark.paimon.write.use-v2-write" -> "true",
      "spark.paimon.sql.dynamic-partition-column-order" -> "table"
    ) {
      withDynamicPartitionTable {
        val hiveOrderInsert =
          s"""
             |INSERT OVERWRITE $targetTableName PARTITION (ds, part)
             |SELECT 'u1' AS uid, 'v1' AS value, '2026-08-10' AS ds, 'p1' AS part
             |""".stripMargin

        assert(
          createDataset(spark, analyzedWriteQuery(hiveOrderInsert)).collect().toSeq == Seq(
            Row("u1", "v1", "2026-08-10", "p1")))
      }
    }
  }

  test("invalid dynamic partition column order fails clearly") {
    withSparkSQLConf("spark.paimon.sql.dynamic-partition-column-order" -> "unknown") {
      val error = intercept[IllegalArgumentException] {
        OptionUtils.dynamicPartitionColumnOrder()
      }
      assert(error.getMessage.contains("Supported values are AUTO, TABLE, and HIVE"))
    }
  }

  test("invalid dynamic partition column order does not affect non-partitioned writes") {
    withSparkSQLConf(
      "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
      "spark.paimon.write.use-v2-write" -> "true",
      "spark.paimon.sql.dynamic-partition-column-order" -> "unknown"
    ) {
      withTable("non_partitioned_order") {
        sql("CREATE TABLE non_partitioned_order (id INT, value STRING)")
        sql("INSERT INTO non_partitioned_order VALUES (1, 'v1')")
        checkAnswer(sql("SELECT id, value FROM non_partitioned_order"), Row(1, "v1"))
        sql("INSERT OVERWRITE non_partitioned_order VALUES (2, 'v2')")
        checkAnswer(sql("SELECT id, value FROM non_partitioned_order"), Row(2, "v2"))
      }
    }
  }
}
