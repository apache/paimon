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

package org.apache.spark.sql.paimon

import org.apache.spark.benchmark.Benchmark

import java.io.OutputStream

import scala.concurrent.duration._

case class PaimonBenchmark(
    name: String,
    valuesPerIteration: Long,
    minNumIters: Int = 2,
    warmupTime: FiniteDuration = 2.seconds,
    minTime: FiniteDuration = 2.seconds,
    outputPerIteration: Boolean = false,
    output: Option[OutputStream] = None)
  extends Benchmark(
    name,
    valuesPerIteration,
    minNumIters,
    warmupTime,
    minTime,
    outputPerIteration,
    output)
