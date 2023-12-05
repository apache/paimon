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

import org.apache.paimon.CoreOptions.MergeEngine

sealed trait RowLevelOp {
  val supportedMergeEngine: Seq[MergeEngine]
}

case object Delete extends RowLevelOp {
  override def toString: String = "delete"

  override val supportedMergeEngine: Seq[MergeEngine] = Seq(MergeEngine.DEDUPLICATE)
}

case object Update extends RowLevelOp {
  override def toString: String = "update"

  override val supportedMergeEngine: Seq[MergeEngine] =
    Seq(MergeEngine.DEDUPLICATE, MergeEngine.PARTIAL_UPDATE)
}

case object MergeInto extends RowLevelOp {
  override def toString: String = "merge into"

  override val supportedMergeEngine: Seq[MergeEngine] =
    Seq(MergeEngine.DEDUPLICATE, MergeEngine.PARTIAL_UPDATE)
}
