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

import org.apache.paimon.CoreOptions
import org.apache.paimon.options.Options
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class PaimonV2WriteBuilder(table: FileStoreTable, dataSchema: StructType, options: Options)
  extends BaseV2WriteBuilder(table) {

  override def build: PaimonV2Write = {
    val finalTable = overwriteDynamic match {
      case Some(o) =>
        table.copy(Map(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key -> o.toString).asJava)
      case _ => table
    }
    new PaimonV2Write(finalTable, overwritePartitions, copyOnWriteScan, dataSchema, options)
  }

  override def partitionRowType(): RowType = table.schema().logicalPartitionType()
}
