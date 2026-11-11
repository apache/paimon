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

import org.apache.paimon.CoreOptions.BucketFunctionType
import org.apache.paimon.spark.{PaimonSparkTestBase, SparkTable}

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.AppendDataExecV1

class CustomBucketFunctionTest extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {

  private def createTable(name: String, primaryKey: Boolean, custom: Boolean = true): Unit = {
    val key = if (primaryKey) "'primary-key' = 'pt,id'," else ""
    val provider = if (custom) {
      "'bucket-function.provider' = 'spark-test-shifted',"
    } else {
      ""
    }
    sql(s"""CREATE TABLE $name (pt STRING, payload STRING, id INT)
           |PARTITIONED BY (pt) TBLPROPERTIES (
           |$key $provider 'bucket' = '4', 'bucket-key' = 'id')""".stripMargin)
  }

  test("custom provider does not advertise V2 write with the default bucket enum") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("custom_bucket", "default_bucket") {
        createTable("custom_bucket", primaryKey = true)
        createTable("default_bucket", primaryKey = true, custom = false)
        val table = loadTable("custom_bucket")
        assert(table.coreOptions().bucketFunctionType() == BucketFunctionType.DEFAULT)
        assert(SparkTable(loadTable("default_bucket")).useV2Write == gteqSpark3_4)
        assert(!SparkTable(table).useV2Write)
        assert(!SparkTable(table).capabilities().contains(TableCapability.BATCH_WRITE))
      }
    }
  }

  for (primaryKey <- Seq(false, true); useV2 <- Seq(false, true)) {
    test(s"custom provider assigns persisted buckets: primaryKey=$primaryKey, useV2=$useV2") {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2.toString) {
        withTable("custom_bucket") {
          createTable("custom_bucket", primaryKey)
          val inserted = sql("""INSERT INTO custom_bucket VALUES
                               |('p', 'a', 0), ('p', 'b', 1),
                               |('q', 'c', 2), ('q', 'd', 3)""".stripMargin)
          val plan = inserted.queryExecution.executedPlan
            .asInstanceOf[CommandResultExec]
            .commandPhysicalPlan
          assert(collect(plan) { case append: AppendDataExecV1 => append }.size == 1)
          // The provider maps id 0,1,2,3 to buckets 1,2,3,0, respectively.
          checkAnswer(
            sql("SELECT id, __paimon_bucket FROM custom_bucket"),
            Seq(Row(0, 1), Row(1, 2), Row(2, 3), Row(3, 0)))
          // Equality pruning must agree with the writer's provider-based assignment.
          checkAnswer(sql("SELECT payload FROM custom_bucket WHERE id = 0"), Row("a"))
        }
      }
    }
  }

  test("custom provider scan does not advertise default bucket partitioning") {
    assume(gteqSpark3_3)
    withSparkSQLConf(
      "spark.paimon.write.use-v2-write" -> "false",
      "spark.paimon.scan.preserve-data-grouping" -> "true",
      "spark.sql.sources.v2.bucketing.enabled" -> "true") {
      withTable("custom_bucket", "default_bucket") {
        createTable("custom_bucket", primaryKey = true)
        createTable("default_bucket", primaryKey = true, custom = false)
        Seq("custom_bucket", "default_bucket").foreach {
          name => sql(s"INSERT INTO $name VALUES ('p', 'a', 0), ('p', 'b', 1)")
        }
        assert(
          getPaimonScan("SELECT * FROM default_bucket").outputPartitioning
            .isInstanceOf[KeyGroupedPartitioning])
        val scan = getPaimonScan("SELECT * FROM custom_bucket")
        assert(scan.outputPartitioning.isInstanceOf[UnknownPartitioning])
        if (gteqSpark3_4) {
          assert(scan.outputOrdering().isEmpty)
        }
      }
    }
  }

  for (bucketFunction <- Seq("mod", "hive")) {
    test(s"$bucketFunction scan does not advertise default bucket partitioning") {
      assume(gteqSpark3_3)
      withSparkSQLConf(
        "spark.paimon.write.use-v2-write" -> "false",
        "spark.paimon.scan.preserve-data-grouping" -> "true",
        "spark.sql.sources.v2.bucketing.enabled" -> "true") {
        withTable("typed_bucket") {
          sql(s"""CREATE TABLE typed_bucket (id INT, payload STRING) TBLPROPERTIES (
                 |'primary-key' = 'id', 'bucket' = '4',
                 |'bucket-function.type' = '$bucketFunction')""".stripMargin)
          sql("INSERT INTO typed_bucket VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')")
          val scan = getPaimonScan("SELECT * FROM typed_bucket")
          assert(scan.outputPartitioning.isInstanceOf[UnknownPartitioning])
          checkAnswer(sql("SELECT payload FROM typed_bucket WHERE id = 1"), Row("b"))
        }
      }
    }
  }
}
