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

import org.apache.paimon.CoreOptions.{MERGE_ENGINE, MergeEngine}
import org.apache.paimon.options.Options
import org.apache.paimon.table.Table

sealed trait RowLevelOp {

  val name: String = this.getClass.getSimpleName.stripSuffix("$")

  protected val supportedMergeEngine: Seq[MergeEngine]

  protected val supportAppendOnlyTable: Boolean

  def checkValidity(table: Table): Unit = {
    if (!supportAppendOnlyTable && table.primaryKeys().isEmpty) {
      throw new UnsupportedOperationException(s"Only support to $name table with primary keys.")
    }

    val mergeEngine = Options.fromMap(table.options).get(MERGE_ENGINE)
    if (!supportedMergeEngine.contains(mergeEngine)) {
      throw new UnsupportedOperationException(
        s"merge engine $mergeEngine can not support $name, currently only ${supportedMergeEngine
            .mkString(", ")} can support $name.")
    }
  }
}

case object Delete extends RowLevelOp {

  override val supportedMergeEngine: Seq[MergeEngine] = Seq(
    MergeEngine.DEDUPLICATE,
    MergeEngine.PARTIAL_UPDATE,
    MergeEngine.AGGREGATE,
    MergeEngine.FIRST_ROW)

  override val supportAppendOnlyTable: Boolean = true

}

case object Update extends RowLevelOp {

  override val supportedMergeEngine: Seq[MergeEngine] =
    Seq(MergeEngine.DEDUPLICATE, MergeEngine.PARTIAL_UPDATE)

  override val supportAppendOnlyTable: Boolean = true

}

case object MergeInto extends RowLevelOp {

  override val supportedMergeEngine: Seq[MergeEngine] =
    Seq(MergeEngine.DEDUPLICATE, MergeEngine.PARTIAL_UPDATE)

  override val supportAppendOnlyTable: Boolean = false

}
