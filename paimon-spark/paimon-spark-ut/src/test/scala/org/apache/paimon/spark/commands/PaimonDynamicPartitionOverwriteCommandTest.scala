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

package org.apache.paimon.spark.commands

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.PaimonUtils.{createDataset, createNewDataFrame}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import java.io.File

class PaimonDynamicPartitionOverwriteCommandTest extends PaimonSparkTestBase {

  import testImplicits._

  test("dynamic overwrite consumes optimizer-safe child plan") {
    withTempDir {
      tempDir =>
        withTable("paimon_target") {
          try {
            sql("CREATE TABLE paimon_target (id INT, pt STRING) PARTITIONED BY (pt)")
            sql("INSERT INTO paimon_target VALUES (3, 'p3')")

            val srcPath = new File(tempDir, "parquet_src").getCanonicalPath
            val dimPath = new File(tempDir, "dim").getCanonicalPath
            Seq((1, "p1"), (2, "p2"), (3, "p3"))
              .toDF("id", "pt")
              .write
              .partitionBy("pt")
              .parquet(srcPath)
            spark.read.parquet(srcPath).createOrReplaceTempView("parquet_src")

            Seq(("p1", "use"), ("p2", "use"), ("p3", "skip"))
              .toDF("pt", "tag")
              .write
              .parquet(dimPath)
            spark.read.parquet(dimPath).createOrReplaceTempView("dim")

            withSparkSQLConf(
              "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
              "spark.paimon.write.use-v2-write" -> "false",
              "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true",
              "spark.sql.optimizer.dynamicPartitionPruning.useStats" -> "false",
              "spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio" -> "1.0",
              "spark.sql.autoBroadcastJoinThreshold" -> "10485760",
              "spark.sql.adaptive.enabled" -> "false"
            ) {
              val insertSql =
                """
                  |INSERT OVERWRITE paimon_target
                  |SELECT /*+ BROADCAST(dim) */ s.id + 100 AS id, s.pt
                  |FROM parquet_src s JOIN dim ON s.pt = dim.pt
                  |WHERE dim.tag = 'use'
                  |""".stripMargin

              val parsed = spark.sessionState.sqlParser.parsePlan(insertSql)
              val analyzed =
                spark.sessionState.analyzer.executeAndCheck(parsed, new QueryPlanningTracker)
              val cmd = analyzed.asInstanceOf[PaimonDynamicPartitionOverwriteCommand]

              val optimizedChild = spark.sessionState.optimizer.execute(cmd.query)
              assert(
                hasDynamicPruningSubquery(optimizedChild),
                s"Expected dynamic pruning in optimized child, got:\n$optimizedChild")

              val cmdWithOptimizedQuery = cmd.copy(query = optimizedChild)
              val writeDataFrame =
                createNewDataFrame(createDataset(spark, cmdWithOptimizedQuery.query))
              assert(
                !hasDynamicPruningSubquery(writeDataFrame.queryExecution.logical),
                s"Expected writer DataFrame to be free of dynamic pruning, got:\n" +
                  writeDataFrame.queryExecution.logical
              )

              cmdWithOptimizedQuery.run(spark)
              checkAnswer(
                sql("SELECT * FROM paimon_target ORDER BY id"),
                Seq(Row(3, "p3"), Row(101, "p1"), Row(102, "p2")))
            }
          } finally {
            spark.catalog.dropTempView("parquet_src")
            spark.catalog.dropTempView("dim")
          }
        }
    }
  }

  private def hasDynamicPruningSubquery(plan: LogicalPlan): Boolean = {
    plan.exists(_.expressions.exists(_.exists {
      case _: DynamicPruningSubquery => true
      case _ => false
    }))
  }
}
