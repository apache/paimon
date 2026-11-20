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
import org.apache.paimon.table.BucketMode

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery

import java.io.File

abstract class DynamicPartitionPruningWriteTestBase extends PaimonSparkTestBase {

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.catalog.paimon.cache-enabled", "false")
  }

  for {
    bucketCount <- Seq(-1, 2)
    adaptive <- Seq(false, true)
    pruningEnabled <- Seq(false, true)
  } {
    test(s"V1 insert and update with bucket=$bucketCount, AQE=$adaptive, DPP=$pruningEnabled") {
      withSparkSQLConf(
        "spark.paimon.write.use-v2-write" -> "false",
        "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> pruningEnabled.toString,
        "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly" -> "false",
        "spark.sql.optimizer.dynamicPartitionPruning.useStats" -> "false",
        "spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio" -> "1.0",
        "spark.sql.adaptive.enabled" -> adaptive.toString,
        "spark.sql.shuffle.partitions" -> "2"
      ) {
        withTempDir {
          dir =>
            withTempView("dpp_source", "dpp_left", "dpp_right") {
              withTable("dpp_target") {
                sql(s"""
                       |CREATE TABLE dpp_target (id INT, pt STRING, left_value INT, right_value INT)
                       |PARTITIONED BY (pt)
                       |TBLPROPERTIES ('primary-key' = 'id,pt', 'bucket' = '$bucketCount',
                       |  'dynamic-bucket.assigner-parallelism' = '2')
                       |""".stripMargin)
                val bucketMode =
                  if (bucketCount == -1) BucketMode.HASH_DYNAMIC else BucketMode.HASH_FIXED
                assert(loadTable("dpp_target").bucketMode() == bucketMode)

                val rows = Seq((1, "p1", 10), (2, "p2", 20), (3, "p3", 30))
                val extraRows = (4 until 104).map(i => (i, "p4", i))
                Seq("dpp_source", "dpp_left", "dpp_right").foreach {
                  name =>
                    val input = name match {
                      case "dpp_source" => rows
                      case "dpp_left" => rows ++ extraRows
                      case _ => rows.filterNot(_._1 == 2) ++ extraRows
                    }
                    val path = new File(dir, name).getCanonicalPath
                    input.toDF("id", "pt", "metric").write.partitionBy("pt").parquet(path)
                    spark.read.parquet(path).createOrReplaceTempView(name)
                }

                Seq(0, 100).foreach {
                  increment =>
                    val query = s"""
                                   |SELECT a.id, a.pt, b.metric + $increment AS left_value,
                                   |       c.metric AS right_value
                                   |FROM dpp_source a
                                   |LEFT JOIN dpp_left b ON a.id = b.id AND a.pt = b.pt
                                   |LEFT JOIN dpp_right c ON a.id = c.id AND a.pt = c.pt
                                   |WHERE a.id < 3
                                   |""".stripMargin
                    val plan = sql(query).queryExecution.optimizedPlan
                    val pruning = plan.flatMap(_.expressions).flatMap {
                      expression =>
                        expression.collect { case subquery: DynamicPruningSubquery => subquery }
                    }
                    assert(
                      pruning.nonEmpty == pruningEnabled,
                      s"Unexpected DPP in input plan:\n$plan")
                    sql(s"INSERT INTO dpp_target $query")
                    checkAnswer(
                      sql("SELECT * FROM dpp_target"),
                      Seq(Row(1, "p1", 10 + increment, 10), Row(2, "p2", 20 + increment, null)))
                }
              }
            }
        }
      }
    }
  }
}
