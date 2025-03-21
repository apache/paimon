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

package org.apache.paimon.spark

import org.apache.paimon.options.Options
import org.apache.paimon.spark.source.SparkV2Write
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.connector.write.{SupportsDynamicOverwrite, SupportsOverwrite, WriteBuilder}
import org.apache.spark.sql.sources.{And, Filter}

import scala.collection.JavaConverters._

private class SparkV2WriteBuilder(table: FileStoreTable, options: Options)
  extends BaseWriteBuilder(table, options)
  with SupportsOverwrite
  with SupportsDynamicOverwrite {

  private var overwriteDynamic = false
  private var overwritePartitions: Map[String, String] = null
  override def build = new SparkV2Write(table, overwriteDynamic, overwritePartitions.asJava)
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
      overwritePartitions = Map.empty[String, String]
    } else {
      overwritePartitions =
        convertPartitionFilterToMap(conjunctiveFilters.get, table.schema.logicalPartitionType())
    }

    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    if (overwritePartitions != null) {
      throw new IllegalArgumentException("Cannot overwrite dynamically and by filter both")
    }

    overwriteDynamic = true
    this
  }
}
