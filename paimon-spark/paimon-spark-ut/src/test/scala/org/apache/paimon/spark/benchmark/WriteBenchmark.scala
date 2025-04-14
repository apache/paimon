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

package org.apache.paimon.spark.benchmark

import org.apache.spark.sql.paimon.PaimonBenchmark

object WriteBenchmark extends PaimonSqlBasedBenchmark {

  private val N = 10L * 1000 * 1000
  private val sourceTable = "source"

  def writeNonPKTable(keys: String, i: Int, benchmark: PaimonBenchmark): Unit = {
    writeTable(isPKTable = false, keys, i, benchmark)
  }

  def writeTable(
      isPKTable: Boolean,
      keys: String,
      bucket: Int,
      benchmark: PaimonBenchmark): Unit = {
    benchmark.addCase(s"write table, isPKTable: $isPKTable, keys: $keys, bucket: $bucket", 3) {
      _ =>
        val tableName = "targetTable"
        val keyProp = if (keys.isEmpty) {
          ""
        } else if (isPKTable) {
          s"'primary-key' = '$keys',"
        } else {
          s"'bucket-key' = '$keys',"
        }
        withTable(tableName) {
          sql(s"""
                 |CREATE TABLE $tableName (id INT, s1 STRING, s2 STRING) USING paimon
                 |TBLPROPERTIES (
                 | $keyProp
                 | 'bucket' = '$bucket'
                 |)
                 |""".stripMargin)
          sql(s"INSERT INTO $tableName SELECT * FROM $sourceTable")
        }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmark = PaimonBenchmark(s"Write paimon table", N, output = output)

    withTempTable(sourceTable) {
      spark
        .range(N)
        .selectExpr("id", "uuid() as s1", "uuid() as s2")
        .createOrReplaceTempView(sourceTable)

      // fixed bucket
      writeNonPKTable("id", 10, benchmark)
      writeNonPKTable("s1", 10, benchmark)
      writeNonPKTable("id,s1", 10, benchmark)

      // unaware bucket
      writeNonPKTable("", -1, benchmark)

      benchmark.run()
    }
  }
}
