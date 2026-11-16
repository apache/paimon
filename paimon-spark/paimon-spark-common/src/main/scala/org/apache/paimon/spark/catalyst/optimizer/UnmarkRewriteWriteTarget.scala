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

package org.apache.paimon.spark.catalyst.optimizer

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation

import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Spark appends an insert-only merge to the relation it also reads, which `PaimonMergeInto` marks
 * for the query auth check of the scan. Only the read needs it, and the write target keeps its own
 * options, so the cache refresh after the write still matches cached reads of the table.
 */
object UnmarkRewriteWriteTarget extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      case a: AppendData =>
        a.table match {
          case r: DataSourceV2Relation
              if r.options.containsKey(CoreOptions.SCAN_REWRITE_OPERATION.key()) =>
            a.withNewTable(PaimonRelation.withoutRewriteOperation(r))
          case _ => a
        }
    }
  }
}
