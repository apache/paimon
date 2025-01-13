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
import org.apache.paimon.spark.commands.WriteIntoPaimonTable
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.sources.InsertableRelation

/** Spark [[V1Write]], it is required to use v1 write for grouping by bucket. */
class SparkWrite(val table: FileStoreTable, saveMode: SaveMode, options: Options) extends V1Write {

  override def toInsertableRelation: InsertableRelation = {
    (data: DataFrame, overwrite: Boolean) =>
      {
        WriteIntoPaimonTable(table, saveMode, data, options).run(data.sparkSession)
      }
  }

  override def toString: String = {
    s"table: ${table.fullName()}, saveMode: $saveMode, options: ${options.toMap}"
  }
}
