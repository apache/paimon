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
import org.apache.paimon.spark.commands.FixedBucketExpression
import org.apache.paimon.utils.ExceptionUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCapability, TableCatalog}
import org.apache.spark.sql.execution.{CommandResultExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, AppendDataExecV1}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

class V2WriteRequireDistributionTest extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.spark_catalog", "org.apache.paimon.spark.SparkGenericCatalog")
      .set("spark.sql.catalog.spark_catalog.warehouse", tempDBDir.getCanonicalPath)
  }

  test("V2 write primary key table") {

    withTable("t1") {
      spark.sql(
        "CREATE TABLE t1 (type STRING, id INT, data STRING) partitioned by (type) TBLPROPERTIES ('primary-key' = 'type,id', 'bucket'='2', 'file.format'='avro')")
      val query =
        "INSERT INTO t1 VALUES ('foo', 1, 'x1'), ('foo',1, 'X1'), ('foo', 2, 'x2'), ('foo', 2, 'X2'), ('bar', 3, 'x3'), ('bar', 3, 'X3')"

      withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
        val df = spark.sql(query)
        val nodes = collect(
          df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan) {
          case shuffle: ShuffleExchangeLike => shuffle
          case append: AppendDataExec => append
        }

        assert(nodes.size == 2)

        val node1 = nodes(0)
        assert(
          node1.isInstanceOf[AppendDataExec],
          s"Expected AppendDataExec, but got: $node1"
        )
        // Inspect the `write` field directly. Spark 4.1 overrides
        // `V2ExistingTableWriteExec.stringArgs` with a single-use `Iterator` held in a `val`,
        // which gets exhausted after the first `toString` — subsequent renderings omit the write.
        val writeDesc = node1.asInstanceOf[AppendDataExec].write.toString
        assert(
          writeDesc.contains("PaimonWrite(table=test.t1"),
          s"Expected PaimonWrite, but got write: $writeDesc\nnode: $node1"
        )

        val node2 = nodes(1)
        assert(
          node2.isInstanceOf[ShuffleExchangeLike] &&
            hasBucketFunction(node2),
          s"Expected ShuffleExchangeLike with BucketFunction, but got: $node2"
        )

        checkAnswer(
          spark.sql("SELECT * FROM t1"),
          Seq(
            Row("foo", 1, "X1"),
            Row("foo", 2, "X2"),
            Row("bar", 3, "X3")
          ))
      }
    }
  }

  test("V2 write append only table") {

    withTable("t1") {
      spark.sql(
        "CREATE TABLE t1 (type STRING, id INT, data STRING) partitioned by (type) TBLPROPERTIES ('bucket-key' = 'id', 'bucket'='2', 'file.format'='avro')")
      val query =
        "INSERT INTO t1 VALUES ('foo', 1, 'x1'), ('foo',1, 'X1'), ('foo', 2, 'x2'), ('foo', 2, 'X2'), ('bar', 3, 'x3'), ('bar', 3, 'X3')"

      withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
        val df = spark.sql(query)
        val nodes = collect(
          df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan) {
          case shuffle: ShuffleExchangeLike => shuffle
          case append: AppendDataExec => append
        }

        assert(nodes.size == 2)

        val node1 = nodes(0)
        assert(
          node1.isInstanceOf[AppendDataExec],
          s"Expected AppendDataExec, but got: $node1"
        )
        // Inspect the `write` field directly. Spark 4.1 overrides
        // `V2ExistingTableWriteExec.stringArgs` with a single-use `Iterator` held in a `val`,
        // which gets exhausted after the first `toString` — subsequent renderings omit the write.
        val writeDesc = node1.asInstanceOf[AppendDataExec].write.toString
        assert(
          writeDesc.contains("PaimonWrite(table=test.t1"),
          s"Expected PaimonWrite, but got write: $writeDesc\nnode: $node1"
        )

        val node2 = nodes(1)
        assert(
          node2.isInstanceOf[ShuffleExchangeLike] &&
            hasBucketFunction(node2),
          s"Expected ShuffleExchangeLike with BucketFunction, but got: $node2"
        )
      }

      checkAnswer(
        spark.sql("SELECT * FROM t1"),
        Seq(
          Row("foo", 1, "x1"),
          Row("foo", 1, "X1"),
          Row("foo", 2, "x2"),
          Row("foo", 2, "X2"),
          Row("bar", 3, "x3"),
          Row("bar", 3, "X3")
        ))
    }
  }

  test("Fallback to v1 write") {

    withTable("t1") {
      spark.sql(
        "CREATE TABLE t1 (id INT, data STRING) partitioned by(data) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='-1')")

      val query = "INSERT INTO t1 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')"

      withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
        val df = spark.sql(query)
        val nodes = collect(
          df.queryExecution.executedPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan) {
          case append: AppendDataExecV1 => append
        }

        assert(nodes.size == 1)
        val node1 = nodes(0)
        assert(
          node1.isInstanceOf[AppendDataExecV1] &&
            node1.toString.contains("AppendDataExecV1 PrimaryKeyFileStoreTable[test.t1]"),
          s"Expected AppendDataExec with specific paimon write, but got: $node1"
        )
      }
    }
  }

  for (enabled <- Seq(false, true); primaryKey <- Seq(false, true); adaptive <- Seq(false, true)) {
    test(
      s"Table-as-select bucket distribution: v2=$enabled, primaryKey=$primaryKey, adaptive=$adaptive") {
      withSparkSQLConf(
        "spark.paimon.write.use-v2-write" -> enabled.toString,
        "spark.sql.shuffle.partitions" -> "4",
        "spark.sql.adaptive.enabled" -> adaptive.toString) {
        val key = if (primaryKey) "'primary-key' = 'id'" else "'bucket-key' = 'id'"
        val definition = s"USING paimon TBLPROPERTIES ($key, 'bucket'='1', 'file.format'='avro')"
        val query = "SELECT CAST(id AS INT) AS id FROM range(0, 40, 1, 4)"
        withTable("t1") {
          sql(s"CREATE TABLE t1 $definition AS $query")
          assertSingleBucketFile()
          checkAnswer(sql("SELECT * FROM t1"), (0 until 40).map(Row(_)))

          sql(s"REPLACE TABLE t1 $definition AS $query")
          assertSingleBucketFile()
          checkAnswer(sql("SELECT * FROM t1"), (0 until 40).map(Row(_)))

          sql(s"CREATE OR REPLACE TABLE t1 $definition AS $query")
          assertSingleBucketFile()
          checkAnswer(sql("SELECT * FROM t1"), (0 until 40).map(Row(_)))
        }
        withTable("t1") {
          sql(s"CREATE OR REPLACE TABLE t1 $definition AS $query")
          assertSingleBucketFile()
          checkAnswer(sql("SELECT * FROM t1"), (0 until 40).map(Row(_)))
        }
      }
    }
  }

  test("Table-as-select bucket distribution with SparkGenericCatalog") {
    val session = spark
    val tableName = s"spark_catalog.$dbName0.generic_ctas"
    val definition =
      "USING paimon TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'file.format'='avro')"
    val query = "SELECT CAST(id AS INT) AS id FROM range(0, 40, 1, 4)"
    for (adaptive <- Seq(false, true)) {
      withSparkSQLConf("spark.sql.adaptive.enabled" -> adaptive.toString) {
        try {
          for (command <- Seq("CREATE TABLE", "REPLACE TABLE", "CREATE OR REPLACE TABLE")) {
            session.sql(s"$command $tableName $definition AS $query")
            val table = session.sessionState.catalogManager
              .catalog("spark_catalog")
              .asInstanceOf[TableCatalog]
              .loadTable(Identifier.of(Array(dbName0), "generic_ctas"))
            assert(table.capabilities().contains(TableCapability.BATCH_WRITE))
            checkAnswer(
              session.sql(s"SELECT COUNT(*) FROM spark_catalog.$dbName0.`generic_ctas$$files`"),
              Row(1L))
            checkAnswer(session.sql(s"SELECT * FROM $tableName"), (0 until 40).map(Row(_)))
          }
        } finally {
          session.sql(s"DROP TABLE IF EXISTS $tableName")
        }
      }
    }
  }

  test("Table-as-select write failure drops the newly created table") {
    withTable("t1") {
      val error = intercept[Exception] {
        sql("""
              |CREATE TABLE t1 USING paimon
              |TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'file.format'='avro') AS
              |SELECT CAST(IF(id = 20, raise_error('expected CTAS failure'), id) AS INT) AS id
              |FROM range(0, 40, 1, 4)
              |""".stripMargin)
      }
      assert(
        ExceptionUtils.findThrowableWithMessage(error, "expected CTAS failure").isPresent,
        ExceptionUtils.stringifyException(error))
      assert(!spark.catalog.tableExists("t1"))
    }
  }

  private def assertSingleBucketFile(): Unit = {
    checkAnswer(sql("SELECT COUNT(*) FROM `t1$files`"), Row(1L))
  }

  private def hasBucketFunction(plan: SparkPlan): Boolean = {
    plan.toString.contains("org.apache.paimon.spark.catalog.functions.BucketFunction") ||
    plan.expressions.exists(_.collect { case _: FixedBucketExpression => true }.nonEmpty)
  }
}
