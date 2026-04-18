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

package org.apache.spark.sql.paimon.shims

/**
 * Re-exports Spark's `MemoryStream` from its Spark-3.x location so that Paimon test code can import
 * it uniformly across Spark major versions. Spark 4.1 relocated `MemoryStream` from
 * `org.apache.spark.sql.execution.streaming` to `...streaming.runtime` (see Spark4 shim).
 */
object memstream {
  type MemoryStream[A] = org.apache.spark.sql.execution.streaming.MemoryStream[A]
  val MemoryStream: org.apache.spark.sql.execution.streaming.MemoryStream.type =
    org.apache.spark.sql.execution.streaming.MemoryStream
}
