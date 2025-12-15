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

import org.apache.paimon.table.Table

import org.apache.spark.sql.connector.write.{SupportsDynamicOverwrite, SupportsOverwrite, WriteBuilder}
import org.apache.spark.sql.sources.{And, Filter}

/** Base class for Paimon V2 write builder. */
abstract class BaseV2WriteBuilder(table: Table)
  extends BaseWriteBuilder(table)
  with SupportsOverwrite
  with SupportsDynamicOverwrite {

  protected var overwriteDynamic = false
  protected var overwritePartitions: Option[Map[String, String]] = None

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    if (overwriteDynamic) {
      throw new IllegalArgumentException("Cannot overwrite dynamically and by filter both")
    }

    failIfCanNotOverwrite(filters)

    val conjunctiveFilters = if (filters.nonEmpty) {
      Some(filters.reduce((l, r) => And(l, r)))
    } else {
      None
    }

    if (isTruncate(conjunctiveFilters.get)) {
      overwritePartitions = Option.apply(Map.empty[String, String])
    } else {
      overwritePartitions =
        Option.apply(convertPartitionFilterToMap(conjunctiveFilters.get, partitionRowType()))
    }

    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    if (overwritePartitions.exists(_.nonEmpty)) {
      throw new IllegalArgumentException("Cannot overwrite dynamically and by filter both")
    }

    overwriteDynamic = true
    overwritePartitions = Option.apply(Map.empty[String, String])
    this
  }
}
