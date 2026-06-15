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

package org.apache.paimon.spark.write

/**
 * Constants for the logical operation type recorded in a Paimon snapshot's `properties` map (key
 * [[OPERATION_PROPERTY]]), complementing the physical `CommitKind`. CTAS / RTAS, whose write Spark
 * builds from a write-options map, carry it as the [[OPERATION_OPTION]] write option instead.
 */
object SnapshotOperation {

  /** Snapshot property key holding the operation type. */
  val OPERATION_PROPERTY: String = "operation"

  /** Write-option key carrying the operation through Spark's CTAS/RTAS write-option map. */
  val OPERATION_OPTION: String = "__paimon.operation"

  val WRITE: String = "WRITE"
  val OVERWRITE: String = "OVERWRITE"
  val DELETE: String = "DELETE"
  val UPDATE: String = "UPDATE"
  val MERGE: String = "MERGE"
  val CREATE_TABLE_AS_SELECT: String = "CREATE TABLE AS SELECT"
  val REPLACE_TABLE_AS_SELECT: String = "REPLACE TABLE AS SELECT"
  val CREATE_OR_REPLACE_TABLE_AS_SELECT: String = "CREATE OR REPLACE TABLE AS SELECT"

  /** Build the commit-properties map carrying the given operation. */
  def asProperties(operation: String): Map[String, String] = Map(OPERATION_PROPERTY -> operation)
}
