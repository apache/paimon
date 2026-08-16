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

import org.apache.paimon.spark.rowops.PaimonCopyOnWriteScan
import org.apache.paimon.table.Table

import org.apache.spark.sql.connector.write.{SupportsDynamicOverwrite, SupportsOverwrite, WriteBuilder}
import org.apache.spark.sql.sources.{And, Filter}

/** Base class for Paimon V2 write builder. */
abstract class BaseV2WriteBuilder(table: Table)
  extends BaseWriteBuilder(table)
  with SupportsOverwrite
  with SupportsDynamicOverwrite {

  protected var overwriteDynamic: Option[Boolean] = None
  protected var overwritePartitions: Option[Map[String, String]] = None
  protected var copyOnWriteScan: Option[PaimonCopyOnWriteScan] = None

  def overwriteFiles(copyOnWriteScan: PaimonCopyOnWriteScan): WriteBuilder = {
    assert(overwriteDynamic.isEmpty && overwritePartitions.isEmpty)
    this.copyOnWriteScan = Some(copyOnWriteScan)
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    assert(overwriteDynamic.isEmpty && copyOnWriteScan.isEmpty)
    failIfCanNotOverwrite(filters)

    overwriteDynamic = Some(false)
    val conjunctiveFilters = filters.reduce((l, r) => And(l, r))
    if (isTruncate(conjunctiveFilters)) {
      overwritePartitions = Some(Map.empty[String, String])
    } else {
      overwritePartitions = Some(
        convertPartitionFilterToMap(conjunctiveFilters, partitionRowType()))
    }
    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    assert(overwritePartitions.isEmpty && copyOnWriteScan.isEmpty)
    overwriteDynamic = Some(true)
    overwritePartitions = Some(Map.empty[String, String])
    this
  }
}
