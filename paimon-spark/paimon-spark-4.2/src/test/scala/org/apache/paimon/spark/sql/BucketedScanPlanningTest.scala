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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec

class BucketedScanPlanningTest extends BucketedScanPlanningTestBase {

  override protected def scanGroupsReadUnits: Boolean = false

  override protected def groupedPartitions(df: DataFrame): Int = {
    collect(df.queryExecution.executedPlan) {
      case group: GroupPartitionsExec if group.output.exists(_.name == "seq") =>
        group.outputPartitioning.numPartitions
    }.headOption.getOrElse(eventScan(df).inputRDD.getNumPartitions)
  }
}
