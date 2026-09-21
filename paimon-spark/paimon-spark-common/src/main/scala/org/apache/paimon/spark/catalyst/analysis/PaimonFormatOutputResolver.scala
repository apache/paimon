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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.format.PaimonFormatTable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy

/** Align LEGACY format writes without enabling missing-column filling or schema evolution. */
object PaimonFormatOutputResolver extends SQLConfHelper {

  private val resolvedQuery = TreeNodeTag[LogicalPlan]("paimon.format.write.resolved-query")

  def isLegacyFormatWrite(write: V2WriteCommand): Boolean = write.table match {
    case relation: DataSourceV2Relation if relation.table.isInstanceOf[PaimonFormatTable] =>
      write.query.resolved && conf.storeAssignmentPolicy == StoreAssignmentPolicy.LEGACY
    case _ => false
  }

  def resolve(write: V2WriteCommand): V2WriteCommand = {
    if (write.getTagValue(resolvedQuery).contains(write.query)) {
      write
    } else {
      val query = Compatibility.resolveTableOutputColumns(
        write.table.name,
        write.table.output,
        write.query,
        write.isByName,
        conf)
      val resolved = Compatibility.withNewQuery(write, query)
      // LEGACY casts may remain nullable for NOT NULL targets. Keep the existing writer-side
      // nullability checks; do not repeatedly align the query or falsify expression nullability.
      // Tag the command rather than its source query, which can be reused for another table.
      resolved.setTagValue(resolvedQuery, query)
      resolved
    }
  }
}
