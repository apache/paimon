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

object BucketFunctionBenchmark extends PaimonSqlBasedBenchmark {

  private val N = 20L * 1000 * 1000

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmark = PaimonBenchmark(s"Bucket function", N, output = output)

    benchmark.addCase("Single int column", 3) {
      _ => spark.range(N).selectExpr("fixed_bucket(10, id)").noop()
    }

    benchmark.addCase("Single string column", 3) {
      _ => spark.range(N).selectExpr("fixed_bucket(10, uuid())").noop()
    }

    benchmark.addCase("Multiple columns", 3) {
      _ => spark.range(N).selectExpr("fixed_bucket(10, id, uuid(), uuid())").noop()
    }

    benchmark.run()
  }
}
